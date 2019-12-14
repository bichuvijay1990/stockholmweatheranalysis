package com.tcs.weatheranalysis.spark.test

import org.apache.spark.sql.SparkSession
import org.scalatest.{ Outcome, fixture }
import java.io.InputStream

/**
 * This is the test class for testing the StockholmTemperature class 
 * it check count of data in the text file 
 *  @author Bichu vijay
 *
 */
class StockholmTemperatureTest extends fixture.FunSuite {
  type FixtureParam = SparkSession

  def withFixture(test: OneArgTest): Outcome = {
    val sparkSession = SparkSession.builder
      .appName("UnitTestRunTemp")
      .master("local")
      .getOrCreate()
    try {
      withFixture(test.toNoArgTest(sparkSession))
    } finally sparkSession.stop
  }

  test("Testing the input file count") { spark =>
    val TempInputRDD = spark.sparkContext.parallelize(readTextfile("/data/temperature_data.txt"), 2)
    assert(TempInputRDD.count === 50)
  }

  private def readTextfile(input: String): Seq[String] = {
    val ipStream: InputStream = getClass.getResourceAsStream(input)
    scala.io.Source.fromInputStream(ipStream).getLines.toSeq
  }
}
