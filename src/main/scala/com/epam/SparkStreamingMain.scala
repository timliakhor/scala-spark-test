package com.epam

import com.epam.streaming.StreamWorker
import org.apache.spark.sql.SparkSession

object SparkStreamingMain {
  def main(args: Array[String]): Unit = {

    val spark = createSparkSession

    new StreamWorker().start(spark)
  }

  def createSparkSession: SparkSession = {
    SparkSession.builder().appName("spark-app1-2")
      .config("spark.master", "local[*]")
      .config("spark.driver.memory", "16g")
      .config("spark.driver.maxResultSize", "0")
      .config("spark.executor.memory", "16g")
      .getOrCreate()
  }
}
