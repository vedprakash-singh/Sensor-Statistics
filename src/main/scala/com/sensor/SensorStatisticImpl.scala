package com.sensor

import org.apache.spark.sql.SparkSession

import java.io.File

class SensorStatisticImpl {

  import org.apache.spark.sql.functions._

  def processedFiles(files: String) = {
    val d = new File(files)
    val fileList = if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
    println("Num of processed files:" + fileList.size)
  }

  def numOfProcessedMeasurements(paths: String) = {
    var sc = SparkSession.builder
      .appName("Sensor Statistics Task")
      .master("local[*]")
      .getOrCreate
    val df = sc
      .read.option("header", "true")
      .option("inferSchema", "true")
      .csv(paths)
    println("sensor data :")
    df.show()

    //how many measurements it processed
    val processed = df.filter(!isnan(col("humidity"))).count()
    println("how many measurements it processed: " + processed)

    //how many measurements failed
    val nonProcessed = df.filter(isnan(col("humidity"))).count()
    println("how many measurements it not processed: " + nonProcessed)

    //`NaN` values are ignored from min/avg/max
    val sensorAggValue = df.na.drop().groupBy("sensor-id")
      .agg(
        min("humidity").as("min"),
        avg("humidity").as("avg"),
        max("humidity").as("max"))
    println("`NaN` values are ignored from min/avg/max: ")
    sensorAggValue.show()

    //Sensors with only `NaN` measurements have min/avg/max as `NaN/NaN/NaN`
    val sensorAggValueWithNaN = df.groupBy("sensor-id")
      .agg(
        min("humidity").as("min"),
        avg("humidity").as("avg"),
        max("humidity").as("max"))
    println("Sensors with only `NaN` measurements have min/avg/max as `NaN/NaN/NaN")
    sensorAggValueWithNaN.show()

    //sorts sensors by highest avg humidity (`NaN` values go last)
    val highest_avg = sensorAggValueWithNaN.select("avg").orderBy(asc("avg"))
    println("sorts sensors by highest avg humidity :")
    highest_avg.show()
  }

}
