package com.weatheranalysis.layout

/**
 * This is the layout for the stockholm Temperature input data 
 *  
 * @author Bichu vijay
 * @version 1.0
 */

class Temperaturelayout {
  
}

/**
 * Temperature layout used for year 1756_1858
 * input data file have leading space comes before first column.
 */
case class TemperatureDataLayoutSpace(
  space:                String,
  year:                 String,
  month:                String,
  day:                  String,
  temperature_morning : String,
  temperature_noon:     String,
  temperature_evening   : String)
  
  
/**
 * Temperature layout used for year 1859-1960
 */
case class TemperatureDataLayoutCommon(
  year:                String,
  month:               String,
  day:                 String,
  temperature_morning: String,
  temperature_noon:    String,
  temperature_evening: String,
  temperature_min:     String,
  temperature_max:     String
  )
  
/**
 * Temperature layout used for year 1961_2012_2013_2017 
 */
case class TemperatureDataLayout(
  year:                             String,
  month:                            String,
  day:                              String,
  temperature_morning:              String,
  temperature_noon:                 String,
  temperature_evening:              String,
  temperature_min:                  String,
  temperature_max:                  String,
  estimatedDiurnalMean:             String)


