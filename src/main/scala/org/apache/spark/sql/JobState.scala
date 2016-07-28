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

package org.apache.spark.sql

/**
 * Job status.
 */
object JobState extends Enumeration {

  val SUBMITTED, PARSEFAILED, PLANFAILED, WAITING, LAUNCHING, RUNNING, FINISHED, STAGE_SUCCESSED, FAILED, KILLED, UNKNOWN = Value

  val FAILED_STATES = Set(PARSEFAILED, PLANFAILED, FAILED, KILLED)
  val SUCCESSED_STATES = Set(FINISHED)
  val FINISHED_STATES = Set(PARSEFAILED, PLANFAILED, FAILED, KILLED, FINISHED)
  val STAGE_SUCCESSED_STATES = Set(STAGE_SUCCESSED)

  type JobState = Value

  def isFailed(state: JobState) = FAILED_STATES.contains(state)
  def isSucceeded(state: JobState) = SUCCESSED_STATES.contains(state)
  def isFinished(state: JobState) = FINISHED_STATES.contains(state)
  def isStageSucceeded(state: JobState) = STAGE_SUCCESSED_STATES.contains(state)
}
