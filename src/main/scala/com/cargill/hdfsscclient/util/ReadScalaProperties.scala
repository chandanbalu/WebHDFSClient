package com.cargill.hdfsscclient.util

import java.util.Properties

/**
 * Created by IntelliJ IDEA.
 *
 * @author : Chandan Balu
 * @created_date : 6/22/2020, Mon
 **/
class ReadScalaProperties {
  /** Utility functions load the Twitter Properties file. */
  @throws[Exception]
  def loadProperties(resourcePath: String): Properties = {
    /** Read the file and load it to the properties. */
    val props = new Properties
    props.load(this.getClass.getResourceAsStream(resourcePath))
    props
  }
}
