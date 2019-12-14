package com.tcs.weatheranalysis.utils

import java.util.Properties
import scala.io.Source

/**
 * Property reader class
 *
 * @author Bichu vijay
 */

class PropertyReader {
  
  /**
   * Reads property file
   *
   * @return Properties
   */
  def readPropertyFile(): Properties = {
    var properties: Properties = null
    val url = getClass.getResource("/jobspark.properties")
    if (url != null) {
      val source = Source.fromURL(url)
      properties = new Properties()
      properties.load(source.bufferedReader())
    }
    properties
  }
  
}


