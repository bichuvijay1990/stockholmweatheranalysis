# Stockholmweatheranalysis

Stockholmweatheranalysis project is loading stockholm temperature and presusre measusred during different timespan in to two hive table.Temperature and presure data is in the text file format and residing in the hdfs directories. After the required cleansing and transformation on the input data by spark and scala , transfomed data loading in to hive table.


### Prerequisites

1. Download input files from following location

    Stockholm Temperature: https://bolin.su.se/data/stockholm/raw_individual_temperature_observations.php

    Stockholm pressure :https://bolin.su.se/data/stockholm/barometer_readings_in_original_units.php

2. File are downloaded and moving in to the hdfs location, hdfs locations specified in the sparkjob.properties file

```
Stockholm Temperature data  
stockholm_daily_temp_obs_1756_1858_t1t2t3.txt  move to the path spark.temp.input.dir.t1t2t3 
stockholm_daily_temp_obs_1859_1960_t1t2t3txtn.txt move to the path spark.temp.input.dir.t1t2t3txtn 
stockholm_daily_temp_obs_1961_2012_t1t2t3txtntm.txt move to the path spark.temp.input.dir.t1t2t3txtntm 
stockholm_daily_temp_obs_2013_2017_t1t2t3txtntm.txt move to the path spark.temp.input.dir.manual 
stockholmA_daily_temp_obs_2013_2017_t1t2t3txtntm move to the path spark.temp.input.dir.automatic 

Stockholm Pressure data data hdfs input path
stockholm_barometer_1756_1858.txt move to the path spark.pressure.1858.input.dir
stockholm_barometer_1859_1861.txt move to the path spark.pressure.1861.input.dir 
stockholm_barometer_1862_1937.txt move to the pathspark.pressure.1937.input.dir 
stockholm_barometer_1938_1960.txt move to the path spark.pressure.1960.input.dir 
stockholm_barometer_1961_2012.txt move to the path spark.pressure.2012.input.dir 
stockholm_barometer_2013_2017.txt move to the path spark.pressure.input.dir.manual 
stockholmA_barometer_2013_2017.txt move to the path spark.pressure.input.dir.automatic 
```

### Installing and Running the tests
1.Build the jar using maven build

2.Move jar to cluster

3.Execute of the test by using below commands

Temperature Analysis
spark-submit --class com.weatheranalysis.spark.StockholmPressure --master yarn <location of weatheranalysis-1.0.0.jar>

Pressure Analaysis
spark-submit --class com.weatheranalysis.spark.StockholmTemperature --master yarn <location of weatheranalysis-1.0.0.jar>
