package org.apache.spark.sql.yarn

import java.nio.ByteBuffer

import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.sql.JobState._
import org.apache.spark.util.SerializableBuffer

/**
 * Created by taguswang on 2016/4/8.
 */

private[spark] sealed trait SQLClientMessage extends Serializable

object SQLClientMessage {

  case class RegisteredDriver(driver: RpcEndpointRef) extends SQLClientMessage

  case class JobProgressResult(progress: Double)
    extends SQLClientMessage

  case class StatusUpdate(state: JobState, data: SerializableBuffer)
    extends SQLClientMessage

  object StatusUpdate {
    /** Alternate factory method that takes a ByteBuffer directly for the data field */
    def apply(state: JobState, data: ByteBuffer)
    : StatusUpdate = {
      StatusUpdate(state, new SerializableBuffer(data))
    }
  }

  case class SQLResults(results: Seq[String], outputSchemaData: SerializableBuffer)
    extends SQLClientMessage

  object SQLResults {
    def apply(results: Seq[String], data: ByteBuffer)
    : SQLResults = {
      SQLResults(results, new SerializableBuffer(data))
    }
  }

}