package org.apache.spark.sql.yarn

import org.apache.spark.Logging

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}

/**
 * Created by taguswang on 2016/4/8.
 */
class JobWaiter extends Logging {

  private val jobPromise: Promise[Unit] = Promise()

  def jobFinished: Boolean = jobPromise.isCompleted

  def completionFuture: Future[Unit] = jobPromise.future

  private var sqlText: String = null

  def setSqlText(sql: String) = {
    sqlText = sql
  }

  def getSqlText(): String = sqlText

  def jobCompleted(): Unit = {
    jobPromise.success(())
  }

  def jobFailed(exception: Exception): Unit = {
    if (!jobPromise.tryFailure(exception)) {
      logWarning("Ignore failure", exception)
    }
  }

  def awaitResults(): JobResult = {
    var result: JobResult = null
    Await.ready(completionFuture, atMost = Duration.Inf)
    completionFuture.value.get match {
      case scala.util.Success(_) =>
        result = JobSucceeded
      case scala.util.Failure(exception) =>
        result = JobFailed(exception)
    }
    return result
  }
}
