package com.tcs.weatheranalysis.layout


/**
 * This is the layout for the stockholm presurre input data 
 *  
 * @author Bichu vijay
 * @version 1.0
 */

class PressureLayout {
  
}


/**
 * Pressure reading schema layout for year 1756_1858
 */
case class Pressure1756Layout(
  year:                                 String,
  month:                                String,
  day:                                  String,
  pressure_morning:                     String,
  barometer_temperature_observations_1: String,
  pressure_noon:                        String,
  barometer_temperature_observations_2: String,
  pressure_evening:                     String,
  barometer_temperature_observations_3: String)

  /**
 * Pressure reading layout for year 1859_1861
 */
case class Pressure1859Layout(
  year:                             String,
  month:                            String,
  day:                              String,
  pressure_morning:                 String,
  thermometer_observations_1:       String,
  air_pressure_reduced_to_0_degC_1: String,
  pressure_noon:                    String,
  thermometer_observations_2:       String,
  air_pressure_reduced_to_0_degC_2: String,
  pressure_evening:                 String,
  thermometer_observations_3:       String,
  air_pressure_reduced_to_0_degC_3: String)

  /**
 * Pressure reading layout for year 1862_1937_1938_1960
 * Input data have leading space
 */
case class Pressure1862layout(
  space:            String,
  year:             String,
  month:            String,
  day:              String,
  pressure_morning: String,
  pressure_noon:    String,
  pressure_evening: String)
  
/**
 * Pressure reading data layout for 1961_2012_2013_2017
 * manual and automatic station
 */
case class PressureCommonLayout(
  year:             String,
  month:            String,
  day:              String,
  pressure_morning: String,
  pressure_noon:    String,
  pressure_evening: String)



  


     
