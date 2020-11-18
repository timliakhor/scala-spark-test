package com.epam

import com.epam.batching.BatchingHotelJoinExpediaTask
import org.apache.spark.sql.SparkSession

object SparkBatchingMain {
  def main(args: Array[String]): Unit = {

    val spark = createSparkSession

    new BatchingHotelJoinExpediaTask().joinHotelAndWeather(spark)
  }

  def createSparkSession: SparkSession = {
    SparkSession.builder().appName("example-spark-scala")
      .config("spark.master", "local[*]").getOrCreate()
  }
}
