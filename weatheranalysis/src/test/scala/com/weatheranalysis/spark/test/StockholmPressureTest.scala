package com.weatheranalysis.spark.test

import org.apache.spark.sql.SparkSession
import org.scalatest.{ Outcome, fixture }
import java.io.InputStream

/**
 * This is the test class for testing the StockholmPresure class 
 *  @author Bichu vijay
 *
 */
class StockholmPressureTest extends fixture.FunSuite {
  type FixtureParam = SparkSession

  def withFixture(test: OneArgTest): Outcome = {
    val sparkSession = SparkSession.builder
      .appName("UnitTestRun")
      .master("local")
      .getOrCreate()
    try {
      withFixture(test.toNoArgTest(sparkSession))
    } finally sparkSession.stop
  }

  test("Testing the input file count") { spark =>
    val pressureInputRDD = spark.sparkContext.parallelize(readTextfile("/data/pressure_data.txt"), 2)
    assert(pressureInputRDD.count === 42)
  }

  private def readTextfile(input: String): Seq[String] = {
    val ipStream: InputStream = getClass.getResourceAsStream(input)
    scala.io.Source.fromInputStream(ipStream).getLines.toSeq
  }
}