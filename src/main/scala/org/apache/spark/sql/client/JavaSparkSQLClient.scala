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

package org.apache.spark.sql.client

import java.io.IOException

import scala.collection.mutable.HashMap

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.{SQLUtils, JobState}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.util.Utils

class JavaSparkSQLClient(sparkConf: SparkConf) extends Logging {
  var initialYarnSparkSQLClient = false

  lazy val sparkContextClient: SQLClient = try {
    val clazz =
      Class.forName("org.apache.spark.sql.yarn.YarnSQLClient")
    val cons = clazz.getConstructor(classOf[SparkConf])
    val scc = cons.newInstance(sparkConf).asInstanceOf[SQLClient]
    initialYarnSparkSQLClient = true
    scc
  } catch {
    case e: Exception => {
      throw new IOException("YarnSparkContextClient init failed", e)
    }
  }

  lazy val serializer = SQLUtils.instantiateClass[Serializer](sparkConf,
    sparkConf.get("spark.closure.serializer", "org.apache.spark.serializer.JavaSerializer"))

  def submitSql(sqlText: String, args: Array[String], outputPath: String = null): Boolean = {
    // TODO parseSQL at first
//    try {
//      val logicalPlan = HiveQl.parseSql(sqlText)
//      if (logicalPlan == null) {
//        return false
//      }
//    } catch {
//    }
    return sparkContextClient.submitSql(sqlText, args, outputPath)
  }


  def getJobTrackerUrl(): String = {
    if (initialYarnSparkSQLClient) {
      sparkContextClient.getJobTrackerUrl
    } else {
      null
    }
  }

  def getJobProgress(): Double = {
    if (initialYarnSparkSQLClient) {
      sparkContextClient.getJobProgress()
    } else {
      0.0
    }
  }

  def stop(shouldKillApp: Boolean = false) {
    if (initialYarnSparkSQLClient) {
      sparkContextClient.stop(shouldKillApp)
    }
  }

  def isComplete(): Boolean = {
    if (initialYarnSparkSQLClient) {
      sparkContextClient.isComplete()
    } else {
      false
    }
  }

  def isSuccessful(): Boolean = {
    if (initialYarnSparkSQLClient) {
      sparkContextClient.isSuccessful()
    } else {
      false
    }
  }

  def getState(): JobState.JobState = {
    if (initialYarnSparkSQLClient) {
      sparkContextClient.getState()
    } else {
      JobState.UNKNOWN
    }
  }

  def getResults() : Seq[String] = {
    sparkContextClient.getJobResults()
  }

  def getCurrentSchema(): Seq[Attribute] = {
    val data = sparkContextClient.getCurrentSchemaData()
    if (data.value.limit() > 0) {
      val ser = serializer.newInstance()
      ser.deserialize[Seq[Attribute]](data.value)
    } else {
      Seq.empty
    }
  }

  def getReason(): Throwable = {
    if (initialYarnSparkSQLClient) {
      sparkContextClient.getReason()
    } else {
      null
    }
  }

  def getAppID(): String = {
    if (initialYarnSparkSQLClient) {
      sparkContextClient.getAppID()
    } else {
      null
    }
  }

  def awaitCompletion(): Unit = {
    sparkContextClient.awaitCompletion()
  }

  def monitorAndPrintState(): Boolean = {
    val threadId = Thread.currentThread().getId
    var lastReport: String = null
    val interval: Int = sparkConf.getInt("spark.client.state.monitor.interval", 1000)
    val maxReportInterval = 60 * 1000
    var reportTime = System.currentTimeMillis()
    var reportedAfterCompletion: Boolean = false
    while (!isComplete() || !reportedAfterCompletion) {
      if (isComplete()) {
        reportedAfterCompletion = true
      } else {
        Thread.sleep(interval)
      }

      var report: String = getState().toString()
      val reason = getReason()
      val appId = getAppID()
      if (reason != null) {
        report = report + " " + reason.getMessage()
      }
      if (appId != null) {
        report = appId + ": " + report
      }

      if (!report.equals(lastReport) ||
        System.currentTimeMillis() > (reportTime + maxReportInterval)) {
        val msg = s"${threadId} ===> ${report}"
        logInfo(msg)

        lastReport = report
        reportTime = System.currentTimeMillis()
      }
    }
    val success: Boolean = isSuccessful()
    if (success) {
      val msg = s"${threadId} ===> This sql completed successfully"
      logInfo(msg)

    } else {
      val msg = s"${threadId} ===> This sql failed with state" +
        s" ${getState()} due to: ${getReason()}"
      logInfo(msg)
    }
    success
  }
}

object JavaSparkSQLClient extends Logging {

  def main(argStrings: Array[String]): Unit = {
    val defaultProperties = new HashMap[String, String]()
    val propertiesFile = argStrings(0)

    println(s"Using properties file: $propertiesFile")
    Utils.getPropertiesFromFile(propertiesFile).foreach { case (k, v) =>
      defaultProperties(k) = v
      println(s"Adding default property: $k=$v")
    }

    val sparkConf = new SparkConf()
    defaultProperties.foreach { case (k, v) =>
      sparkConf.set(k, v);
    }
    val client = new JavaSparkSQLClient(sparkConf)
    val tempPath = argStrings(1) + "/temp-" +System.currentTimeMillis()
    val sql = argStrings(2)
    client.submitSql(sql, Array("user", "password","dbName"), tempPath)

    try {
      client.awaitCompletion()
    } catch {
    case e: Exception =>
      println("exception::" + e)
    }

    if (client.isSuccessful()) {
      val outputSchema = client.getCurrentSchema()
      println("outputSchema:")
      outputSchema.foreach(result => println(outputSchema))

      val results = client.getResults()
      println("results:")
      results.foreach(result => println(result))

    } else {
      println("error's reason::" + client.getReason())
    }

    client.stop(false)
    println("job end.")
  }
}


