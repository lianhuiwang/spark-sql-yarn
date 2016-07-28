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

import org.apache.spark.Logging

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}

/**
 *  An object that waits for a SQL job to complete.
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
