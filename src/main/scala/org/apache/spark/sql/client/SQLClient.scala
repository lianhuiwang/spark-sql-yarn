package org.apache.spark.sql.client

import org.apache.spark.sql.JobState
import org.apache.spark.util.SerializableBuffer

/**
 * Created by taguswang on 2016/4/6.
 */
trait SQLClient {
  def stop(userKilled: Boolean = false): Unit
  def submitSql(sqlText: String, args: Array[String], outputPath: String = null): Boolean
  def awaitCompletion(): Unit
  def getState() : JobState.JobState
  def getReason() : Exception
  def getJobResults() : Seq[String]
  def isComplete() : Boolean
  def isSuccessful(): Boolean
  def getCurrentSchemaData(): SerializableBuffer
  def getJobTrackerUrl() : String
  def getAppID() : String
  def getJobProgress(): Double
}
