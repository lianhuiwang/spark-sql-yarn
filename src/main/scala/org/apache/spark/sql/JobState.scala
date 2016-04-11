package org.apache.spark.sql

/**
 * Created by taguswang on 2016/4/6.
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
