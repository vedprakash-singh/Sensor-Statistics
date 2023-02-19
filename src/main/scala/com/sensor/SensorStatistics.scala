package com.sensor

import org.apache.log4j.{Level, Logger}

object SensorStatistics extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val dir = ("src/resources/")
  val sensorSpark = new SensorStatisticImpl()
  sensorSpark.processedFiles(dir)
  sensorSpark.numOfProcessedMeasurements(dir)

}
