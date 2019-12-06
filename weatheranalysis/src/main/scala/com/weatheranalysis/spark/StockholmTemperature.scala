package com.weatheranalysis.spark

import java.io.FileNotFoundException
import scala.util.Failure
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

import com.weatheranalysis.spark.helper.SparkConfiguration
import com.weatheranalysis.layout.TemperatureDataLayoutCommon
import com.weatheranalysis.layout.TemperatureDataLayoutSpace
import com.weatheranalysis.layout.TemperatureDataLayout
import com.weatheranalysis.utils.PropertyReader

/**
 * This is the main class contain the Entry point of the temperature application 
 * read data from text file, add additional column and loading in to hive
 *  
 * @author Bichu vijay
 * @version 1.0
 */

object StockholmTemperature extends SparkConfiguration {
  
  val log = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) {
    //Naming the spark session
    spark.conf.set("spark.app.name", "Stockholm Temperature Analysis")
    temperatureAnalysis(spark)
    stopSpark()
  }

  def temperatureAnalysis(sparkSession: SparkSession): Unit = {

    // property file read
    val propertiesReader = new PropertyReader()
    val properties = propertiesReader.readPropertyFile()

    try {
         
      //Space contained temperature data t1t2t3 
      //Read input data
      val t1t2t3TempDataRDD = sparkSession.sparkContext
        .textFile(properties.getProperty("spark.temp.input.dir.t1t2t3"))
        .map(x => x.split("\\s+"))
        .map(x => TemperatureDataLayoutSpace(x(0), x(1), x(2), x(3), x(4), x(5), x(6)))
      val t1t2t3TempDataDF = sparkSession.createDataFrame(t1t2t3TempDataRDD)
      // Add necessary columns to the input 
      val spaceTempCleansedDF = t1t2t3TempDataDF
        .drop("space")
        .withColumn("temperature_min", lit("NaN"))
        .withColumn("temperature_max", lit("NaN"))
        .withColumn("estimatedDiurnalMean", lit("NaN"))
        .withColumn("station", lit("NaN"))
        .withColumn("temperature_unit", lit("degC"))
               
      //Temperature data t1t2t3txtn  
      val t1t2t3txtnTempDataRDD = sparkSession.sparkContext
        .textFile(properties.getProperty("spark.temp.input.dir.t1t2t3txtn"))
        .map(x => x.split("\\s+"))
        .map(x => TemperatureDataLayoutCommon(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7)))
      val t1t2t3txtnTempDataDF = sparkSession.createDataFrame(t1t2t3txtnTempDataRDD)
      val t1t2t3txtnTempDF = t1t2t3txtnTempDataDF
        .withColumn("estimatedDiurnalMean", lit("NaN"))
        .withColumn("station", lit("NaN"))
        .withColumn("temperature_unit", lit("degC"))
      
      // Temperature Data t1t2t3txtntm 
      // Read input data
      val t1t2t3txtntmTempDataRDD = sparkSession.sparkContext
        .textFile(properties.getProperty("spark.temp.input.dir.t1t2t3txtntm"))
        .map(x => x.split("\\s+"))
        .map(x =>TemperatureDataLayout(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8)))
      val t1t2t3txtntmTempDataDF = sparkSession.createDataFrame(t1t2t3txtntmTempDataRDD)
      val t1t2t3txtntmTempDF = t1t2t3txtntmTempDataDF
          .withColumn("station", lit("NaN"))
          .withColumn("temperature_unit", lit("degC"))
      
      // Manual Station Temperature Data     
      // Read input data
      val manualStationTempRDD = sparkSession.sparkContext
        .textFile(properties.getProperty("spark.temp.input.dir.manual"))
        .map(x => x.split("\\s+"))
        .map(x =>
          TemperatureDataLayout(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8)))
      val manualStationTempDF = sparkSession.createDataFrame(manualStationTempRDD)
      val manualStationDF = manualStationTempDF
        .withColumn("station", lit("manual"))
        .withColumn("temperature_unit", lit("degC"))

      //Automatic Station Temperature Data
      //Read input data
      val automaticStationTempRDD = sparkSession.sparkContext
        .textFile(properties.getProperty("spark.temp.input.dir.automatic"))
        .map(x => x.split("\\s+"))
        .map(x => TemperatureDataLayout(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8)))
      val automaticStationTempDF = sparkSession.createDataFrame(automaticStationTempRDD)
      val automaticStationDF = manualStationTempDF
        .withColumn("station", lit("automatic"))
        .withColumn("temperature_unit", lit("degC"))

      // using union making all data frames to single one
      val tempDataDF = spaceTempCleansedDF
        .union(t1t2t3txtnTempDF)
        .union(t1t2t3txtntmTempDF)
        .union(manualStationDF)
        .union(automaticStationDF)

     //create hive table and load data in to table
      import spark.sql
     
      sql("""CREATE TABLE Stockholm_Temperature_Data(year String, month String,day String, temperature_morning String, temperature_noon String, temperature_evening String, 
        temperature_min String,temperature_max String, estimated_diurnal_mean String, station String ,temperature_unit String) STORED AS PARQUET""")
      
      // Insert data to hive table 
      tempDataDF.write.mode(SaveMode.Overwrite).saveAsTable("Stockholm_Temperature_Data")

      //Data check
      
      val tempDataCount = t1t2t3TempDataRDD.count()+t1t2t3txtnTempDataRDD.count()+
        t1t2t3txtntmTempDataRDD.count()+
        manualStationTempRDD.count() +
        automaticStationTempRDD.count() 
        
      log.info("Input Temperature data count : " + tempDataCount)
      log.info("Transformed Temperature input data count : " + tempDataDF.count())

    } catch {
      case fileNotFoundException: FileNotFoundException => {
        log.error("Input file not found")
        Failure(fileNotFoundException)
      }
      case exception: Exception => {
        log.error("Exception found " + exception)
        Failure(exception)
      }
    }
  }
  
}