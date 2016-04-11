package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.util.Utils

/**
 * Created by taguswang on 2016/4/8.
 */
object SQLUtils {

  // Create an instance of the class with the given name, possibly initializing it with our conf
  def instantiateClass[T](sparkConf: SparkConf, className: String): T = {
    val cls = Utils.classForName(className)
    // Look for a constructor taking a SparkConf and a boolean isDriver, then one taking just
    // SparkConf, then one taking no arguments
    try {
      cls.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
        .newInstance(sparkConf, new java.lang.Boolean(false))
        .asInstanceOf[T]
    } catch {
      case _: NoSuchMethodException =>
        try {
          cls.getConstructor(classOf[SparkConf]).newInstance(sparkConf).asInstanceOf[T]
        } catch {
          case _: NoSuchMethodException =>
            cls.getConstructor().newInstance().asInstanceOf[T]
        }
    }
  }
}
