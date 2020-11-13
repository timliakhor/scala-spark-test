package com.epam

import org.apache.spark.sql.SparkSession

object SparkBatchingMain {
  def main(args: Array[String]): Unit = {

    val spark = createSparkSession

    new BatchingHotelJoinExpediaTask().joinHotelAndWeather(spark)
  }

  def createSparkSession: SparkSession = {
    SparkSession.builder().appName("example-spark-scala-read")
      .config("spark.master", "local[*]").getOrCreate()
  }
}
