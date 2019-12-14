package com.tcs.weatheranalysis.spark

import java.io.FileNotFoundException
import scala.util.Failure

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

import com.tcs.weatheranalysis.spark.helper.SparkConfiguration
import com.tcs.weatheranalysis.utils.PropertyReader
import com.tcs.weatheranalysis.layout.PressureCommonLayout
import com.tcs.weatheranalysis.layout.Pressure1862layout
import com.tcs.weatheranalysis.layout.Pressure1756Layout
import com.tcs.weatheranalysis.layout.Pressure1859Layout

/**
 * This is the main class contain the Entry point of the application 
 *  
 * @author Bichu vijay
 * @version 1.0
 */
 
object StockholmPressure extends SparkConfiguration {
  
  val log = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) {

    // Naming spark program
    spark.conf.set("spark.app.name", "Stockholm Pressure Data Analysis")
    pressureAnalysis(spark)
    stopSpark()
  }

  /**
   * Pressure weather data analysis
   *
   * @param sparkSession
   */
  def pressureAnalysis(sparkSession: SparkSession): Unit = {

    // Property file read
    val propertiesReader = new PropertyReader()
    val properties = propertiesReader.readPropertyFile()

    try {
      
      // data frame for stockholm barometer observation 1756_1858
      
      //Read input data
      val pressureData1756RDD = sparkSession.sparkContext
        .textFile(properties.getProperty("spark.pressure.1858.input.dir"))
        .map(x => x.split("\\s+"))
        .map(x => Pressure1756Layout(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8)))
      val pressureData1756TempDF = sparkSession.createDataFrame(pressureData1756RDD)
      // Add necessary columns to the input
      val pressureData1756SchemaDF = pressureData1756TempDF
        .withColumn("station", lit("NaN"))
        .withColumn("pressure_unit", lit("Swedish inches (29.69 mm)"))
        .withColumn("thermometer_observations_1", lit("NaN"))
        .withColumn("thermometer_observations_2", lit("NaN"))
        .withColumn("thermometer_observations_3", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_1", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_2", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_3", lit("NaN"))
      //Reorder the column in to common structure
      val pressureData1756DF = pressureData1756SchemaDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening",
        "station", "pressure_unit", "barometer_temperature_observations_1", "barometer_temperature_observations_2",
        "barometer_temperature_observations_3", "thermometer_observations_1",
        "thermometer_observations_2", "thermometer_observations_3",
        "air_pressure_reduced_to_0_degC_1", "air_pressure_reduced_to_0_degC_2", "air_pressure_reduced_to_0_degC_3")
        
      //data frame for Stockholm barometer observation 1859_1861

      val pressureData1859RDD = sparkSession.sparkContext
        .textFile(properties.getProperty("spark.pressure.1861.input.dir"))
        .map(x => x.split("\\s+"))
        .map(x => Pressure1859Layout(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11)))
      val pressureData1859TempDF = sparkSession.createDataFrame(pressureData1859RDD)
      // Add necessary columns to the input
      val pressureData1859SchemaDF = pressureData1859TempDF
        .withColumn("station", lit("NaN"))
        .withColumn("pressure_unit", lit("0.1*Swedish inches (2.969 mm)"))
        .withColumn("barometer_temperature_observations_1", lit("NaN"))
        .withColumn("barometer_temperature_observations_2", lit("NaN"))
        .withColumn("barometer_temperature_observations_3", lit("NaN"))
      //Reorder the column in to common structure
      val pressureData1859DF = pressureData1859SchemaDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening",
        "station", "pressure_unit", "barometer_temperature_observations_1", "barometer_temperature_observations_2",
        "barometer_temperature_observations_3", "thermometer_observations_1",
        "thermometer_observations_2", "thermometer_observations_3",
        "air_pressure_reduced_to_0_degC_1", "air_pressure_reduced_to_0_degC_2", "air_pressure_reduced_to_0_degC_3")
        
       
      //data frame for Cleansed stockholm barometer observation 1862_1937

      val pressureData1862RDD = sparkSession.sparkContext
        .textFile(properties.getProperty("spark.pressure.1937.input.dir"))
        .map(x => x.split("\\s+"))
        .map(x => Pressure1862layout(x(0), x(1), x(2), x(3), x(4), x(5), x(6)))
      val pressureData1862TempDF = sparkSession.createDataFrame(pressureData1862RDD)
      // Add necessary columns to the input
      val pressureData1862SchemaDF = pressureData1862TempDF
        .drop("space")
        .withColumn("station", lit("NaN"))
        .withColumn("pressure_unit", lit("mmhg"))
        .withColumn("barometer_temperature_observations_1", lit("NaN"))
        .withColumn("barometer_temperature_observations_2", lit("NaN"))
        .withColumn("barometer_temperature_observations_3", lit("NaN"))
        .withColumn("thermometer_observations_1", lit("NaN"))
        .withColumn("thermometer_observations_2", lit("NaN"))
        .withColumn("thermometer_observations_3", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_1", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_2", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_3", lit("NaN"))
      //Reorder the column in to common structure
      val pressureData1862DF = pressureData1862SchemaDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening",
        "station", "pressure_unit", "barometer_temperature_observations_1", "barometer_temperature_observations_2",
        "barometer_temperature_observations_3", "thermometer_observations_1",
        "thermometer_observations_2", "thermometer_observations_3",
        "air_pressure_reduced_to_0_degC_1", "air_pressure_reduced_to_0_degC_2", "air_pressure_reduced_to_0_degC_3")
        
        
      //data frame for Cleansed stockholm barometer observation 1938_1960

      val pressureData1938RDD = sparkSession.sparkContext
        .textFile(properties.getProperty("spark.pressure.1960.input.dir"))
        .map(item => item.split("\\s+"))
        .map(x => Pressure1862layout(x(0), x(1), x(2), x(3), x(4), x(5), x(6)))
      val pressureData1938TempDF = sparkSession.createDataFrame(pressureData1938RDD)
      // Add necessary columns to the input
      val pressureData1938SchemaDF = pressureData1938TempDF
        .drop("space")
        .withColumn("station", lit("NaN"))
        .withColumn("pressure_unit", lit("hpa"))
        .withColumn("barometer_temperature_observations_1", lit("NaN"))
        .withColumn("barometer_temperature_observations_2", lit("NaN"))
        .withColumn("barometer_temperature_observations_3", lit("NaN"))
        .withColumn("thermometer_observations_1", lit("NaN"))
        .withColumn("thermometer_observations_2", lit("NaN"))
        .withColumn("thermometer_observations_3", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_1", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_2", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_3", lit("NaN"))
      //Reorder the column in to common structure
      val pressureData1938DF = pressureData1938SchemaDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening",
        "station", "pressure_unit", "barometer_temperature_observations_1", "barometer_temperature_observations_2",
        "barometer_temperature_observations_3", "thermometer_observations_1",
        "thermometer_observations_2", "thermometer_observations_3",
        "air_pressure_reduced_to_0_degC_1", "air_pressure_reduced_to_0_degC_2", "air_pressure_reduced_to_0_degC_3")
        
      //data frame for Stockholm barometer observation 1961_2012
      val pressureData1961RDD = sparkSession.sparkContext
        .textFile(properties.getProperty("spark.pressure.2012.input.dir"))
        .map(x => x.split("\\s+"))
        .map(x => PressureCommonLayout(x(0), x(1), x(2), x(3), x(4), x(5)))
      val pressureData1961TempDF = sparkSession.createDataFrame(pressureData1961RDD)
      // Add necessary columns to the input
      val pressureData1961SchemaDF = pressureData1961TempDF
        .withColumn("station", lit("NaN"))
        .withColumn("pressure_unit", lit("hpa"))
        .withColumn("barometer_temperature_observations_1", lit("NaN"))
        .withColumn("barometer_temperature_observations_2", lit("NaN"))
        .withColumn("barometer_temperature_observations_3", lit("NaN"))
        .withColumn("thermometer_observations_1", lit("NaN"))
        .withColumn("thermometer_observations_2", lit("NaN"))
        .withColumn("thermometer_observations_3", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_1", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_2", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_3", lit("NaN"))

      val pressureData1961DF = pressureData1961SchemaDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening",
        "station", "pressure_unit", "barometer_temperature_observations_1", "barometer_temperature_observations_2",
        "barometer_temperature_observations_3", "thermometer_observations_1",
        "thermometer_observations_2", "thermometer_observations_3",
        "air_pressure_reduced_to_0_degC_1", "air_pressure_reduced_to_0_degC_2", "air_pressure_reduced_to_0_degC_3")

      //data frame for  Stockholm barometer observation for Manual Station Pressure Data 2013_2017
      val manualPressureDataRDD = sparkSession
        .sparkContext
        .textFile(properties.getProperty("spark.pressure.input.dir.manual"))
        .map(item => item.split("\\s+"))
        .map(column => PressureCommonLayout(column(0), column(1), column(2), column(3), column(4), column(5)))

      val manualPressureDataTempDF = sparkSession.createDataFrame(manualPressureDataRDD)
      val manualPressureSchemaDF = manualPressureDataTempDF
        .withColumn("station", lit("manual"))
        .withColumn("pressure_unit", lit("hpa"))
        .withColumn("barometer_temperature_observations_1", lit("NaN"))
        .withColumn("barometer_temperature_observations_2", lit("NaN"))
        .withColumn("barometer_temperature_observations_3", lit("NaN"))
        .withColumn("thermometer_observations_1", lit("NaN"))
        .withColumn("thermometer_observations_2", lit("NaN"))
        .withColumn("thermometer_observations_3", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_1", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_2", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_3", lit("NaN"))
      //Reorder the column in to common structure
      val manualPressureDataDF = manualPressureSchemaDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening",
        "station", "pressure_unit", "barometer_temperature_observations_1", "barometer_temperature_observations_2",
        "barometer_temperature_observations_3", "thermometer_observations_1",
        "thermometer_observations_2", "thermometer_observations_3",
        "air_pressure_reduced_to_0_degC_1", "air_pressure_reduced_to_0_degC_2", "air_pressure_reduced_to_0_degC_3")
    
      //data frame for Stockholm barometer observation for Automatic Station Pressure Data 2013_2017
     
      val automaticPressureRDD = sparkSession.sparkContext
        .textFile(properties.getProperty("spark.pressure.input.dir.automatic"))
        .map(x => x.split("\\s+"))
        .map(x => PressureCommonLayout(x(0), x(1), x(2), x(3), x(4), x(5)))
      val automaticPressureTempDF = sparkSession.createDataFrame(automaticPressureRDD)
      val automaticPresssureSchemaDF = automaticPressureTempDF
        .withColumn("station", lit("Automatic"))
        .withColumn("pressure_unit", lit("hpa"))
        .withColumn("barometer_temperature_observations_1", lit("NaN"))
        .withColumn("barometer_temperature_observations_2", lit("NaN"))
        .withColumn("barometer_temperature_observations_3", lit("NaN"))
        .withColumn("thermometer_observations_1", lit("NaN"))
        .withColumn("thermometer_observations_2", lit("NaN"))
        .withColumn("thermometer_observations_3", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_1", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_2", lit("NaN"))
        .withColumn("air_pressure_reduced_to_0_degC_3", lit("NaN"))
        //Reorder the column in to common structure
      val automaticPressureDataDF = automaticPresssureSchemaDF.select(
        "year", "month", "day", "pressure_morning", "pressure_noon", "pressure_evening",
        "station", "pressure_unit", "barometer_temperature_observations_1", "barometer_temperature_observations_2",
        "barometer_temperature_observations_3", "thermometer_observations_1",
        "thermometer_observations_2", "thermometer_observations_3",
        "air_pressure_reduced_to_0_degC_1", "air_pressure_reduced_to_0_degC_2", "air_pressure_reduced_to_0_degC_3")
  
      // using union making all data frames to single one
      val pressureDF = manualPressureDataDF
        .union(automaticPressureDataDF)
        .union(pressureData1938DF)
        .union(pressureData1862DF)
        .union(pressureData1756DF)
        .union(pressureData1859DF)
        .union(pressureData1961DF)

      // Save pressure data to hive table
      import spark.sql
      sql("""CREATE TABLE Stockholm_Pressure_Data(
            year String, 
            month String, 
            day String, 
            pressure_morning String, 
            pressure_noon String, 
            pressure_evening String, 
            station String, 
            pressure_unit String, 
            barometer_temperature_observations_1 String, 
            barometer_temperature_observations_2 String, 
            barometer_temperature_observations_3 String, 
            thermometer_observations_1 String, 
            thermometer_observations_2 String, 
            thermometer_observations_3 String, 
            air_pressure_reduced_to_0_degC_1 String, 
            air_pressure_reduced_to_0_degC_2 String, 
            air_pressure_reduced_to_0_degC_3 String) 
          STORED AS PARQUET""")
      pressureDF.write.mode(SaveMode.Overwrite).saveAsTable("Stockholm_Pressure_Data")

      // Data integrity check
      val presureInputCount = manualPressureDataRDD.count() +
        automaticPressureRDD.count() +
        pressureData1938RDD.count() +
        pressureData1862RDD.count() +
        pressureData1756RDD.count() +
        pressureData1859RDD.count() +
        pressureData1961RDD.count()
      log.info("Stockholm pressure Input data count is " + presureInputCount)
      log.info("Transformed input data count is " + pressureDF.count())

    } catch {
      case fileNotFoundException: FileNotFoundException => {
        log.error("File not found")
        Failure(fileNotFoundException)
      }
      case exception: Exception => {
        log.error("Exception found " + exception)
        Failure(exception)
      }
    }
  }
  
}