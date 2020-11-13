package com.epam

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, datediff, from_json, from_unixtime, lag, month, sum, to_date, unix_timestamp, when, year}
import org.apache.spark.sql.types.{DataTypes, IntegerType, LongType, StringType, StructField, StructType}
import org.slf4j.{Logger, LoggerFactory}

class BatchingHotelJoinExpediaTask {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def joinHotelAndWeather(spark: SparkSession): Unit = {
    import spark.implicits._
    val hotelStructDf = readHotelFromKafka(spark)
    val expediaDf = readAvroExpedia(spark).persist()

    val aggregateExpediaIdleDaysDf = calculateIdleDays(spark, expediaDf).persist()

    val joinData = hotelStructDf.join(aggregateExpediaIdleDaysDf, $"hotel.id" === $"clean_hotel_id", "inner")
      .persist()

    groupByCity(joinData)

    groupByCountry(joinData)

    writeExpediaInAvro(spark, expediaDf.join(aggregateExpediaIdleDaysDf, $"hotel_id" === $"clean_hotel_id", "inner"))
  }

  private def groupByCountry(joinData: DataFrame) = {
    logger.info("checkin`s count by country")
    joinData.groupBy("hotel.country").agg(sum("checkin_count") as "checkin_count").show()

  }

  private def groupByCity(joinData: DataFrame) = {
    logger.info("checkin`s count by cities")
    joinData.groupBy("hotel.city").agg(sum("checkin_count") as "checkin_count").show()
  }

  private def readHotelFromKafka(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val hotelDf = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", PropertyReader.getProperty("kafka_url"))
      .option("subscribe", PropertyReader.getProperty("kafka_hotel_topic"))
      .option("startingOffsets", PropertyReader.getProperty("start_kafka_offset"))
      .option("endingOffsets", PropertyReader.getProperty("end_kafka_offset"))
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

  private def calculateIdleDays(spark: SparkSession, expediaDf: DataFrame): DataFrame = {
    import spark.implicits._
    val window = Window.orderBy(col("hotel_id").asc,
      col("srch_ci").asc, col("srch_co").asc)
    var lastCheckout = lag(col("srch_co"), 1).over(window)
    val previousHotelId = lag(col("hotel_id"), 1).over(window)
    var dateDiff = datediff(to_date(col("srch_ci")),
      to_date(col("last_checkout")))
    lastCheckout = when(previousHotelId =!= col("hotel_id"), 0)
      .otherwise(lastCheckout)
    dateDiff = when(dateDiff.<(0) || dateDiff.isNull, 0)
      .otherwise(dateDiff)
    val expediaResult = expediaDf.withColumn("last_checkout", lastCheckout)
      .withColumn("date_diff", dateDiff)
      .withColumn("previous_hotel_id", previousHotelId)
      .where("srch_ci is not null and srch_co is not null")
      .groupBy(col("hotel_id") as "clean_hotel_id")
      .agg(sum($"date_diff") as "idle_days", count($"hotel_id") as "checkin_count").orderBy(col("checkin_count").desc)
      .persist()

    expediaResult
  }

  private def readAvroExpedia(spark: SparkSession): DataFrame = {
    spark.read
      .format("avro")
      .load(PropertyReader.getProperty("expedia_url"))
  }

  private def writeExpediaInAvro(spark: SparkSession, expediaDf: DataFrame) = {
    import spark.implicits._
    expediaDf
      .withColumn("date_month", month(from_unixtime(unix_timestamp($"srch_ci", "yyyy-MM-dd"))))
      .withColumn("date_year", year(from_unixtime(unix_timestamp($"srch_ci", "yyyy-MM-dd"))))
      .write
      .partitionBy("date_year","date_month")
      .mode(SaveMode.Overwrite).format("avro").save(PropertyReader.getProperty("clean_expedia_url"))
  }

}
