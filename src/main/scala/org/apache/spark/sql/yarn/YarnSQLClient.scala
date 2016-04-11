package org.apache.spark.sql.yarn

import java.lang.{Boolean => JBoolean}
import java.net.InetAddress
import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.yarn.api.records.{FinalApplicationStatus, ApplicationId, YarnApplicationState}

import org.apache.spark.{SecurityManager, SparkConf, Logging}
import org.apache.spark.deploy.yarn.{Client, ClientArguments}
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.client.SQLClient
import org.apache.spark.sql.{SQLUtils, JobState}
import org.apache.spark.sql.JobState.JobState
import org.apache.spark.sql.yarn.SQLClientMessage._
import org.apache.spark.sql.yarn.SQLMasterMessage._
import org.apache.spark.rpc._
import org.apache.spark.util.{SerializableBuffer, Utils}

/**
 * Created by taguswang on 2016/4/6.
 */
class YarnSQLClient(config: SparkConf)  extends SQLClient with Logging {

  def this() = this(new SparkConf())
  private[spark] val sparkConf = config.clone()


  private var client: Client = null
  private var appId: ApplicationId = null
  private val driverRegistered : AtomicReference[JBoolean] =
    new AtomicReference[JBoolean](null)

  private var rpcEnv: RpcEnv = null
  private var driverEndpoint: RpcEndpointRef = null
  private var driverClientEndpoint: RpcEndpointRef = null
  private var monitorThread: Thread = null
  private var jobTrackerUrl: String = ""
  private var jobState: JobState.Value = JobState.UNKNOWN
  private var reason: Exception = null

  private var jobProgress: Double = 0.0
  private var jobResults: Seq[String] = Seq("success")
  private var outputSchemaData: SerializableBuffer = null

  private val jobWaiter: JobWaiter = new JobWaiter()

  lazy val serializer = SQLUtils.instantiateClass[Serializer](sparkConf,
    sparkConf.get("spark.closure.serializer", "org.apache.spark.serializer.JavaSerializer"))

  val ser = serializer.newInstance()

  class DriverClientEndpoint(val rpcEnv: RpcEnv, conf: SparkConf)
    extends ThreadSafeRpcEndpoint with Logging {

    override def receive: PartialFunction[Any, Unit] = {
      case RegisteredDriver(driver) =>
        Utils.checkHostPort(driver.address.hostPort, "Host port expected " + driver.address.hostPort)
        logInfo(s"Registered driver: ${driver.address}")

        driverEndpoint = driver
        notifyRegisterDriver(true)

      case StatusUpdate(state, data) =>
        statusUpdate(state, data)
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case JobProgressResult(progress) =>
        if (jobProgress < progress) {
          jobProgress = progress
        }
        context.reply(true)

      case SQLResults(results, schemaData) =>
        jobResults = results
        outputSchemaData = schemaData
        context.reply(true)
    }

    override def onDisconnected(remoteAddress: RpcAddress): Unit = {
      val msg = s"$appId Driver $remoteAddress disassociated!"
      logError(msg)
      if (reason == null) {
        reason = new Exception(msg)
      }
      jobWaiter.jobFailed(reason)
    }
  }

  def statusUpdate(state: JobState, serializedData: SerializableBuffer) = {
    jobState = state
    if (JobState.isFailed(state)) {
      reason = ser.deserialize[Exception](serializedData.value)
      jobWaiter.jobFailed(reason)
      logError(s"state is $state, reason is $reason")
    } else if (JobState.isFinished(state)) {
      jobWaiter.jobCompleted()
      logError(s"state is $state")
    }
  }

  private def notifyRegisterDriver(s: Boolean) {
    driverRegistered.synchronized {
      driverRegistered.compareAndSet(null, s)
      driverRegistered.notifyAll()
    }
  }

  private def waitRegisterDriver() {
    driverRegistered.synchronized {
      while (driverRegistered.get() == null) {
        try {
          driverRegistered.wait()
        } catch {
          case e: InterruptedException =>

        }
      }
      logInfo(s"$appId got register driver jobState: ${driverRegistered.get()}")
    }
  }

  // launching when initializing
  start()

  private def start(): Unit = {
    // start driver client actor
    val actorSystemName = "SparkSQLClient"
    val hostName = Utils.localHostName()
    val port = 0
    rpcEnv = RpcEnv.create(actorSystemName, hostName, port, sparkConf, new SecurityManager(sparkConf))
    driverClientEndpoint = rpcEnv.setupEndpoint("SparkSQLClient", new DriverClientEndpoint(rpcEnv, sparkConf))

    logInfo(s"start SparkSQLClient, hostPort= ${driverClientEndpoint.address}")
    val hostPort = driverClientEndpoint.address.hostPort

    // launch yarn app
    val argStrings = new ArrayBuffer[String]()
    argStrings += ("--class", "org.apache.spark.sql.yarn.SQLApplicationMaster")
    argStrings += ("--jar", sparkConf.get("spark.sql.yarn.files"))
    if (sparkConf.get("spark.sql.yarn.archives", null) != null) {
      argStrings += ("--archives", sparkConf.get("spark.sql.yarn.archives"))
    }
    argStrings += ("--args", hostPort)
    argStrings += ("--name", sparkConf.get("spark.app.name", "Spark"))
    if (sparkConf.get("spark.executor.instances", null) != null) {
      argStrings += ("--num-executors", sparkConf.get("spark.executor.instances"))
    }
    if (sparkConf.get("spark.executor.memory", null) != null) {
      argStrings += ("--executor-memory", sparkConf.get("spark.executor.memory"))
    }
    if (sparkConf.get("spark.executor.cores", null) != null) {
      argStrings += ("--executor-cores", sparkConf.get("spark.executor.cores"))
    }
    if (sparkConf.get("spark.driver.memory", null) != null) {
      argStrings += ("--driver-memory", sparkConf.get("spark.driver.memory"))
    }
    if (sparkConf.get("spark.sql.yarn.jars", null) != null) {
      argStrings += ("--addJars", sparkConf.get("spark.sql.yarn.jars"))
    }

    val macIp = InetAddress.getLocalHost.getHostAddress.toString()
    sparkConf.set("spark.job.submit.host", macIp)

    val currTime = System.currentTimeMillis()
    sparkConf.set("spark.job.submit.time", String.valueOf(currTime))

    System.setProperty("SPARK_YARN_MODE", "true")

    val args = new ClientArguments(argStrings.toArray, sparkConf)
    client = new Client(args, sparkConf)
    logInfo("begin to submit job")
    appId = client.submitApplication()
    jobTrackerUrl = client.getApplicationReport(appId).getTrackingUrl
    logInfo(s"jobTrackerUrl=$jobTrackerUrl")
    monitorThread = monitorApplication(appId)
    waitRegisterDriver()
  }

  def updateStateForFailedApplication(reason: Exception) = {
    jobState = JobState.FAILED
    notifyRegisterDriver(false)
    jobWaiter.jobFailed(reason)
  }

  def monitorApplication(appId: ApplicationId): Thread = {
    val thread = new Thread {
      override def run() {
        val interval = sparkConf.getLong("spark.yarn.report.interval", 3000)
        while (true) {
          Thread.sleep(interval)
          val report = client.getApplicationReport(appId)
          val newState = report.getYarnApplicationState
          val finalState = report.getFinalApplicationStatus
          if (newState == YarnApplicationState.FAILED ||
            finalState == FinalApplicationStatus.FAILED) {
            logInfo(s"appId: $appId finished with failed status ")
            reason = new Exception("Job " + appId + " FAILED.")
            updateStateForFailedApplication(reason)
            return
          } else if (newState == YarnApplicationState.KILLED ||
            finalState == FinalApplicationStatus.KILLED) {
            logInfo(s"appId: $appId is killed ")
            reason = new Exception("Job " + appId + " KILLED.")
            updateStateForFailedApplication(reason)
            return
          } else if (newState == YarnApplicationState.FINISHED) {
            logInfo(s"appId: $appId got $newState")
            jobState = JobState.FINISHED
            jobWaiter.jobCompleted()
            return
          } else if (newState == YarnApplicationState.RUNNING &&
            jobState == JobState.SUBMITTED) {
            jobState = JobState.RUNNING
          }
        }
      }
    }
    thread.setName("Monitor Driver App State")
    thread.start()
    thread
  }

  override def submitSql(sqlText: String, args: Array[String], outputPath: String): Boolean = {
    var isSelect = false
    if (sqlText.trim.startsWith("select")) {
      isSelect = true
    }
    if (driverEndpoint != null) {
      jobWaiter.setSqlText(sqlText)
      logInfo(s"submitSql sqlText: $sqlText args: $args outputPath: $outputPath isSelect: $isSelect")
      driverEndpoint.send(SQLSubmitted(sqlText, args, outputPath, isSelect))
      true
    } else {
      false
    }
  }

  override def awaitCompletion(): Unit = {
    jobWaiter.awaitResults() match {
      case JobSucceeded =>
        jobState = JobState.FINISHED
        logInfo(s"success sql: ${jobWaiter.getSqlText()}")
      case JobFailed(exception) =>
        jobState = JobState.FAILED
        throw exception
    }
  }

  override def stop(userKilled: Boolean): Unit = {
    try {
      driverEndpoint.askWithRetry[Boolean](StopDriver)
    } catch {
      case e: Exception =>
        logError(s"Error stopping driver actor Exception: $e ")
    }

    if (appId != null && userKilled) {
      val f = classOf[org.apache.spark.deploy.yarn.Client].getDeclaredField("org$apache$spark$deploy$yarn$Client$$yarnClient")
      f.setAccessible(true)
      f.get(client).asInstanceOf[org.apache.hadoop.yarn.client.api.YarnClient].killApplication(appId)
    }

    if (rpcEnv != null) {
      rpcEnv.shutdown()
      rpcEnv = null
    }
  }

  override def getJobTrackerUrl(): String = jobTrackerUrl

  override def getReason(): Exception = reason

  override def isComplete(): Boolean = JobState.isFinished(jobState)

  override def isSuccessful(): Boolean = JobState.isSucceeded(jobState)

  override def getJobProgress(): Double = jobProgress

  override def getAppID(): String = appId.toString

  override def getState(): JobState.JobState = jobState

  override def getCurrentSchemaData(): SerializableBuffer = outputSchemaData

  override def getJobResults(): Seq[String] = jobResults

}
