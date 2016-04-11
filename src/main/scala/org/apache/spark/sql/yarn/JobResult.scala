package org.apache.spark.sql.yarn

/**
 * Created by taguswang on 2016/4/8.
 */
sealed trait JobResult

case object JobSucceeded extends JobResult

case class JobFailed(exception: Throwable) extends JobResult
