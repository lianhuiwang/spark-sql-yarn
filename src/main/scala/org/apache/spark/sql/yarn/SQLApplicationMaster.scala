/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.yarn

import java.net.Socket
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rpc._
import org.apache.spark.sql.JobState
import org.apache.spark.sql.JobState.JobState
import org.apache.spark.sql.yarn.SQLClientMessage._
import org.apache.spark.sql.yarn.SQLMasterMessage._
import org.apache.spark.sql.SQLContext
import org.apache.spark.util.Utils

/**
 * Yarn's ApplicationMaster for a SQL Query.
 */
class SQLApplicationMaster(args: Array[String], sparkConf: SparkConf) extends Logging {

  def this(args: Array[String]) = this(args, new SparkConf())

  val sparkContext: SparkContext = {
    val conf = sparkConf.clone
    conf.set("spark.master", "yarn-cluster")
    new SparkContext(conf)
  }

  val ser = SparkEnv.get.closureSerializer.newInstance()

  private val isHive = sparkConf.getBoolean("spark.sql.yarn.isHive", false)

  private var rpcEnv: RpcEnv = null
  private var amEndpoint: RpcEndpointRef = _
  private var driverClientEndpoint: RpcEndpointRef = _

  var driverClientStoppedRef = new AtomicReference[Boolean](false)
  var lastException: Exception = null

  private val EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new Array[Byte](0))

  class SparkSQLAMEndpoint(val rpcEnv: RpcEnv, clientEndpoint: RpcEndpointRef)
    extends ThreadSafeRpcEndpoint with Logging {

    override def onStart(): Unit = {
      logInfo(s"SparkSQLAMEndpoint start, send RegisteredDriver to ${clientEndpoint.address}")
      clientEndpoint.send(RegisteredDriver(self))
    }

    override def receive: PartialFunction[Any, Unit] = {
      case SQLSubmitted(sqlText, args, out, isSelect) =>
        submitSql(sqlText, args, out, isSelect)
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case StopDriver =>
        logInfo("DriverClient commanded a shutdown")
        driverClientStoppedRef.synchronized {
          driverClientStoppedRef.compareAndSet(false, true)
          driverClientStoppedRef.notifyAll()
        }
        context.reply(true)
    }

    override def onDisconnected(remoteAddress: RpcAddress): Unit = {
      logError(s"Driver client $remoteAddress disassociated!")
      driverClientStoppedRef.synchronized {
        driverClientStoppedRef.compareAndSet(false, true)
        driverClientStoppedRef.notifyAll()
      }
    }
  }

  def sendJobProgressToClient(): Unit = {
    var hasJobStarted = false
    val thread = new Thread {
      override def run() {
        val interval = sparkConf.getLong("spark.sql.jobProgress.report.interval", 3000)
        while (true) {
          try {
            val activeJobs = sparkContext.jobProgressListener.activeJobs

            if (!hasJobStarted && (activeJobs != null && !activeJobs.isEmpty)) {
              hasJobStarted = true
            }

            // calculate progress by job webUI data,
            val progress = {
              if (!hasJobStarted) {
                0.0
              } else {
                if (activeJobs == null || activeJobs.isEmpty) {
                  100.0
                } else {
                  val jobUIData = activeJobs.valuesIterator.next()
                  val totalTasks = jobUIData.numTasks - jobUIData.numSkippedStages
                  val completeTasks = jobUIData.numCompletedTasks
                  completeTasks.toDouble / totalTasks.toDouble
                }
              }
            }

            driverClientEndpoint.askWithRetry[Boolean](JobProgressResult(progress))
            Thread.sleep(interval)
          } catch {
            case e: Exception =>
              logError("sendJobProgressToClient has error: ", e)
              sendJobProgressToClient()
          }
        }
      }
    }
    thread.setName("send jobProgress to client")
    thread.start()
  }

  def statusUpdate(state: JobState, serializedData: ByteBuffer) = {
    driverClientEndpoint.send(StatusUpdate(state, serializedData))
  }

  def submitSql(sqlText: String, args: Array[String], out: String, isSelect: Boolean): Unit = {
    logInfo(s"submitSql isHive: $isHive sqlText: $sqlText args: $args out: $out isSelect: $isSelect ")
    sendJobProgressToClient()
    statusUpdate(JobState.SUBMITTED, EMPTY_BYTE_BUFFER)
    try {
      var outputSchemaData = EMPTY_BYTE_BUFFER
      var results: Seq[String] = Seq.empty
      var sqlContext: SQLContext = null
      if (isHive) {
        val clazz = Class.forName("org.apache.spark.sql.hive.HiveContext")
        val cons = clazz.getConstructor(classOf[SparkContext])
        sqlContext = cons.newInstance(sparkContext).asInstanceOf[SQLContext]
      } else {
        sqlContext = SQLContext.getOrCreate(sparkContext)
      }

      if (isSelect) {
        val execution = sqlContext.executeSql(sqlText)
        val outputSchema = execution.executedPlan.output
        outputSchemaData = ser.serialize(outputSchema)
        execution.executedPlan.execute().saveAsTextFile(out)
      } else {
        val execution = sqlContext.executeSql(sqlText)
        val outputSchema = execution.executedPlan.output
        outputSchemaData = ser.serialize(outputSchema)
        results = execution.executedPlan.executeCollect().map(row => row.toString()).toSeq
      }

      driverClientEndpoint.askWithRetry[Boolean](SQLResults(results, outputSchemaData))
      statusUpdate(JobState.FINISHED, EMPTY_BYTE_BUFFER)
    } catch {
      case e: Exception =>
        lastException = e
        logError("submitSql has error: ", e)
        statusUpdate(JobState.FAILED, ser.serialize(e))
    }
  }

  private def waitForSparkSQLClient() {
    logInfo("Waiting for Spark driver client to be reachable.")
    val numTries = sparkConf.getInt("spark.yarn.applicationMaster.waitTries", 300)
    var driverClientUp = false
    var count = 0
    val hostPort = args(0)
    val (driverClientHost, driverClientPort) = Utils.parseHostPort(hostPort)
    while(!driverClientUp && count < numTries) {
      try {
        count = count + 1
        val socket = new Socket(driverClientHost, driverClientPort)
        socket.close()
        logInfo(s"Driver client now available: $driverClientHost:$driverClientPort")
        driverClientUp = true
      } catch {
        case e : Exception =>
          logError(s"Failed to connect to driver client at $driverClientHost:$driverClientPort, " +
            s"retrying ...")
          Thread.sleep(100)
      }
    }

    if (!driverClientUp) {
      throw new SparkException("Failed to connect to driver client!")
    }

    rpcEnv =  RpcEnv.create("SparkSQLAM",
      Utils.localHostName(), 0, sparkConf, new SecurityManager(sparkConf))
    driverClientEndpoint = rpcEnv.setupEndpointRef("SparkSQLClient",
      RpcAddress(driverClientHost, driverClientPort), "SparkSQLClient")

    amEndpoint =
      rpcEnv.setupEndpoint("SparkSQLAM", new SparkSQLAMEndpoint(rpcEnv, driverClientEndpoint))
  }

  def run(): Unit = {
    waitForSparkSQLClient()
    logInfo("finish waitForSparkSQLClient...")
    driverClientStoppedRef.synchronized {
      while (driverClientStoppedRef.get() == false) {
        driverClientStoppedRef.wait()
      }
    }
    rpcEnv.shutdown()
    if (lastException != null) {
      throw lastException
    }
  }
}

object SQLApplicationMaster extends Logging {
  val submittingLock: Object = new Object()
  val driverClientStoppedRef = new AtomicReference[Boolean](false)
  var lastException: Throwable = null
  def main(args: Array[String]) {
    logInfo(s"args.length: ${args.length}")
    SparkHadoopUtil.get.runAsSparkUser { () =>
      new SQLApplicationMaster(args).run()
    }
  }
}
