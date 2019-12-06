package com.weatheranalysis.spark.helper

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

/**
 * Trait to define spark session
 *
 * @author Bichu vijay
 */

trait SparkConfiguration {

  lazy val spark =
    SparkSession
      .builder()
      .enableHiveSupport()
      .config("spark.master", "local")
      .getOrCreate()

  /**
   *  Metod to stop the spark session
   */
  def stopSpark(): Unit = spark.stop()
}