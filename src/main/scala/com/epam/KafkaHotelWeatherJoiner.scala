package com.epam

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, datediff, from_json, lag, sum, to_date, when}
import org.apache.spark.sql.types.{DataTypes, StructType}

class KafkaHotelWeatherJoiner {
  def joinHotelAndWeather(spark: SparkSession): Unit = {
    import spark.implicits._
    val hotelStructDf = readHotelFromKafka(spark)
    val expediaDf = readExpediaDataAndCalculateIdleDays(spark)



    val joinData = hotelStructDf.join(expediaDf, $"hotel.id" === $"hotel_id","inner")
     // .where("idle_days >= 2 and idle_days < 30")
    //
   // hotelStructDf.where($"hotel.country" === "US").show()
    joinData.groupBy("hotel.country").agg(sum("checkin_count") as "checkin_count").show()
//
//
//    joinData.foreach(
//      row => println(row)
//    )
//    joinData.show()
  }

  private def readHotelFromKafka(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val hotelDf = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "hotel_kafka")
     // .option("startingOffsets", " {\"hotel_kafka\":{\"0\":10}}")
   //   .option("endingOffsets", " {\"hotel_kafka\":{\"0\":20}}")
      //.option("maxOffsetsPerTrigger", "5")
      .load()

    val hotelJsonDf = hotelDf.selectExpr("CAST(value AS STRING)")

    val struct = new StructType()
      .add("id", DataTypes.LongType)
      .add("name", DataTypes.StringType)
      .add("country", DataTypes.StringType)
      .add("city", DataTypes.StringType)
      .add("address", DataTypes.StringType)

    hotelJsonDf.select(from_json($"value", struct).as("hotel"))
  }

  private def readExpediaDataAndCalculateIdleDays(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val expediaDf: DataFrame = spark.read
      .format("avro")
      .load("hdfs://localhost:9000/user/spark/expedia/*.avro")

    val window = Window.orderBy(col("hotel_id").asc,
      col("srch_ci").asc)//, col("srch_co").asc)
    var lastCheckout = lag(col("srch_co"), 1).over(window)
    val previousHotelId = lag(col("hotel_id"), 1).over(window)
    var dateDiff = datediff(to_date(col("srch_ci")),
      to_date(col("last_checkout")))
    lastCheckout = when(previousHotelId === col("hotel_id"), lastCheckout)
      .otherwise(0)
    dateDiff = when(dateDiff.< (0), 0)
      .otherwise(dateDiff)
    val expediaResult = expediaDf.withColumn("last_checkout", lastCheckout)
      .withColumn("date_diff", dateDiff)
      .withColumn("previous_hotel_id", previousHotelId)
      .where("srch_ci is not null and srch_co is not null")
      .groupBy(col("hotel_id"))
      .agg(sum($"date_diff") as "idle_days", count($"hotel_id") as "checkin_count").orderBy(col("checkin_count").desc)



    expediaResult
  }
}
