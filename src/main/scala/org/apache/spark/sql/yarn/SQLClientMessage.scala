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

import java.nio.ByteBuffer

import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.sql.JobState._
import org.apache.spark.util.SerializableBuffer

/**
 * Messages that ApplicationMaster send to SQL AppClient.
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