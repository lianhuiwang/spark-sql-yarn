package org.apache.spark.sql.yarn

/**
 * Created by taguswang on 2016/4/6.
 */

private[spark] sealed trait SQLMasterMessage extends Serializable

object SQLMasterMessage {

  case class SQLSubmitted(sqlText: String, args: Array[String], outputPath: String, isSelect: Boolean)
    extends SQLMasterMessage

  case object StopDriver extends SQLMasterMessage

}
