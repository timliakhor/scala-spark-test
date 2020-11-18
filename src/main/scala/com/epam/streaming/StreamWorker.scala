package com.epam.streaming

import com.epam.util.Settings.Settings
import com.epam.util.{PropertyReader, Settings}
import org.apache.spark.sql.functions.{max, _}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}


class StreamWorker {

  def start(spark: SparkSession) = {
    import spark.implicits._

    val dateDiff = datediff(to_date(col("srch_co")),
      to_date(col("srch_ci")))
    val erroneousData = when(dateDiff <= 0 || dateDiff > 30, 1)
      .otherwise(0)
    val shortStay = when(dateDiff === 1, 1)
      .otherwise(0)
    val standartStay = when(dateDiff >= 2 && dateDiff <= 7, 1)
      .otherwise(0)
    val standartExtendedStay = when(dateDiff > 7 && dateDiff <= 14, 1)
      .otherwise(0)
    val longStay = when(dateDiff > 14 && dateDiff <= 30, 1)
      .otherwise(0)

    val numCols: Array[String] = Array("erroneous_data", "short_stay", "standart_stay",
      "standart_extended_stay", "long_stay")
    val mostPopularStayCount = col("most_popular_stay_cnt_new")

    val mostPopularStayType = when(mostPopularStayCount === $"erroneous_data", "erroneous_data")
      .when(mostPopularStayCount === $"short_stay", "short_stay")
      .when(mostPopularStayCount === $"standart_stay", "standart_stay")
      .when(mostPopularStayCount === $"standart_extended_stay", "standart_extended_stay")
      .otherwise("long_stay")

    val withChildren = when($"children_cnt" > 0, true)
      .otherwise(false)

    readAvroStreamFromHdfs(spark, Structures.initialJoinExpediaDateStruct, Settings.expedia17JoinWeatherHotelUrl)
      .withColumn("date_diff_2017", dateDiff)
      .withColumn("erroneous_data_2017", erroneousData)
      .withColumn("short_stay_2017", shortStay)
      .withColumn("standart_stay_2017", standartStay)
      .withColumn("standart_extended_stay_2017", standartExtendedStay)
      .withColumn("long_stay_2017", longStay)
      .withColumn("timestamp", current_timestamp)
      .withWatermark("timestamp", "2 minutes")
      .groupBy(col("hotel_id"), functions.window(col("timestamp"), "2 minutes"))
      .agg(max(col("erroneous_data_cnt")) as "erroneous_data_cnt_2016" ,
        max(col("short_stay_cnt")) as "short_stay_cnt_2016",
        max(col("standart_stay_cnt")) as "standart_stay_cnt_2016",
        max(col("standart_extended_stay_cnt")) as "standart_extended_stay_cnt_2016",
        max(col("long_stay_cnt")) as "long_stay_cnt_2016",
        sum($"date_diff_2017") as "durationSum",
        sum($"erroneous_data_2017") as "erroneous_data_cnt_2017",
        sum($"short_stay_2017") as "short_stay_cnt_2017",
        sum($"standart_stay_2017") as "standart_stay_cnt_2017",
        sum($"standart_extended_stay_2017") as "standart_extended_stay_cnt_2017",
        sum($"long_stay_2017") as "long_stay_cnt_2017",
        sum($"srch_children_cnt") as "children_cnt"
      )
      .withColumn("with_children", withChildren)
      .select(
        ($"erroneous_data_cnt_2017" + $"erroneous_data_cnt_2016").as("erroneous_data"),
        ($"short_stay_cnt_2017" + $"short_stay_cnt_2016").as("short_stay"),
        ($"standart_stay_cnt_2017" + $"standart_stay_cnt_2016").as("standart_stay"),
        ($"standart_extended_stay_cnt_2017" + $"standart_extended_stay_cnt_2016").as("standart_extended_stay"),
        ($"long_stay_cnt_2017" + $"long_stay_cnt_2016").as("long_stay"),
      $"children_cnt", $"with_children")
      .withColumn("most_popular_stay_cnt_new", greatest(numCols.head, numCols.tail: _*))
      .withColumn("most_popular_stay_type_new", mostPopularStayType)
      .coalesce(1)
      .writeStream.outputMode(OutputMode.Complete())
    .foreachBatch { (batchDF: DataFrame, batchId: Long) => {
      batchDF.write.format("avro").mode(SaveMode.Append).save(PropertyReader.getProperty(Settings.resultStateUrl))
    }
    }.start()

    spark.streams.awaitAnyTermination()
  }

  private def writeExpedia17JoinInitialData(spark: SparkSession) = {
    import spark.implicits._
    readInitialState(spark)
      .join(readAvroExpediaStream(spark, "2017"), $"hotel_id" === $"group_hotel_id", "inner")
      .coalesce(1)
      .writeStream
      .option("checkpointLocation", "/tmp/checkpoint/saveExp17_1")
      .format("avro")
      .start(PropertyReader.getProperty(Settings.expedia17JoinWeatherHotelUrl))
  }


  private def readInitialState(spark: SparkSession): DataFrame = {
    readAvroStreamFromHdfs(spark, Structures.initialDateStruct, Settings.initialStateUrl)
  }

  private def saveInitialState(spark: SparkSession): Unit = {
    import spark.implicits._
    val dateDiff = datediff(to_date(col("srch_co")),
      to_date(col("srch_ci")))
    val erroneousData = when(dateDiff <= 0 || dateDiff > 30, 1)
      .otherwise(0)
    val shortStay = when(dateDiff === 1, 1)
      .otherwise(0)
    val standartStay = when(dateDiff >= 2 && dateDiff <= 7, 1)
      .otherwise(0)
    val standartExtendedStay = when(dateDiff > 7 && dateDiff <= 14, 1)
      .otherwise(0)
    val longStay = when(dateDiff > 14 && dateDiff <= 30, 1)
      .otherwise(0)

    val numCols: Array[String] = Array("erroneous_data_cnt", "short_stay_cnt", "standart_stay_cnt", "standart_extended_stay_cnt", "long_stay_cnt")

    val mostPopularStayCount = col("most_popular_stay_cnt")

    val mostPopularStayType = when(mostPopularStayCount === $"erroneous_data_cnt", "erroneous_data")
      .when(mostPopularStayCount === $"short_stay_cnt", "short_stay")
      .when(mostPopularStayCount === $"standart_stay_cnt", "standart_stay")
      .when(mostPopularStayCount === $"standart_extended_stay_cnt", "standart_extended_stay")
      .otherwise("long_stay")

    readAvroWeatherHotelExpediaJoinStream(spark)
      .withColumn("date_diff", dateDiff)
      .withColumn("erroneous_data", erroneousData)
      .withColumn("short_stay", shortStay)
      .withColumn("standart_stay", standartStay)
      .withColumn("standart_extended_stay", standartExtendedStay)
      .withColumn("long_stay", longStay)
      .withColumn("timestamp", current_timestamp)
      .withWatermark("timestamp", "10 minutes")
      .groupBy(col("hotel_id") as "group_hotel_id", functions.window(col("timestamp"), "10 minutes"))
      .agg(sum($"date_diff") as "durationSum", sum($"erroneous_data") as "erroneous_data_cnt",
        sum($"short_stay") as "short_stay_cnt", sum($"standart_stay") as "standart_stay_cnt",
        sum($"standart_extended_stay") as "standart_extended_stay_cnt", sum($"long_stay") as "long_stay_cnt", max("timestamp") as "timestamp")
      .withColumn("most_popular_stay_cnt", greatest(numCols.head, numCols.tail: _*))
      .withColumn("most_popular_stay_type", mostPopularStayType)
      .coalesce(1)
      .writeStream
      .outputMode(OutputMode.Complete())
      .foreachBatch { (batchDF: DataFrame, batchId: Long) => {
        batchDF.write.format("avro").mode(SaveMode.Append).save(PropertyReader.getProperty(Settings.initialStateUrl))
      }
      }.start()
  }

  private def writeWeatherHotelJoinResultInAvro(spark: SparkSession) = {
    import spark.implicits._
    readHotelFromKafka(spark)
      .join(readWeatherAggFromKafka(spark), $"hotelResult.geoHash" === $"weatherAggResult.geoHash", "inner")
      .select(col("hotelResult.geoHash") as "geoHash", col("hotelResult.name") as "name",
        col("hotelResult.country") as "country", col("hotelResult.city") as "city",
        col("hotelResult.address") as "address", col("hotelResult.id") as "idPk",
        col("weatherAggResult.weatherDate") as "weatherDate", col("weatherAggResult.averageTemperature") as "averageTemperature")
      .writeStream
      .option("checkpointLocation", "/tmp/checkpoint/save4")
      .format("avro")
      .start(PropertyReader.getProperty(Settings.hotelWeatherJoinUrl))

  }

  private def readAvroStreamFromHdfs(spark: SparkSession, structType: StructType, path: Settings): DataFrame = {
    spark.readStream
      .schema(structType)
      .format("avro")
      .load(PropertyReader.getProperty(path))
  }

  private def readAvroWeatherHotelExpediaJoinStream(spark: SparkSession): DataFrame = {
    readAvroStreamFromHdfs(spark, Structures.expediaHotelWeatherJoinStruct, Settings.hotelWeatherExpediaHdfsUrl)
  }

  private def readAvroWeatherHotelJoinResultStream(spark: SparkSession): DataFrame = {
    readAvroStreamFromHdfs(spark, Structures.structWeatherHotelJoin, Settings.hotelWeatherJoinUrl)
  }

  private def readAvroExpediaStream(spark: SparkSession, yearArg: String): DataFrame = {
    readAvroStreamFromHdfs(spark, Structures.expediaStruct, Settings.cleanExpediaHdfsUrl)
      .where(s"date_year == $yearArg")
  }

  private def readWeatherAggFromKafka(spark: SparkSession): DataFrame = {
    readDataFromKafka(spark, Structures.structWeatherAgg, Settings.kafkaAggregateWeatherTopic, "weatherAggResult")
  }

  private def readHotelFromKafka(spark: SparkSession): DataFrame = {
    readDataFromKafka(spark, Structures.structHotel, Settings.hotelTopic, "hotelResult")
  }

  private def readDataFromKafka(spark: SparkSession, structType: StructType, settingsKafkaTopic: Settings, resultAlias: String): DataFrame = {
    import spark.implicits._
    val kafkaDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", PropertyReader.getProperty(Settings.kafkaUrl))
      .option("subscribe", PropertyReader.getProperty(settingsKafkaTopic))
      .option("startingOffsets", PropertyReader.getProperty(Settings.kafkaStartOffset))
      .load()

    val dataJsonDf = kafkaDf.selectExpr("CAST(value AS STRING)")

    dataJsonDf.select(from_json($"value", structType).as(resultAlias))
  }

  /**
   * Call for preparing aggregate data
   * Aggregate weather and save to weather_agg topic
   *
   * @param spark
   */
  private def readAndAggregateWeather(spark: SparkSession): Unit = {
    import spark.implicits._

    val resDf = readDataFromKafka(spark, Structures.structWeather, Settings.weatherTopic, "weatherResult")
      .groupBy(col("weatherResult.geoHash") as "geoHash", col("weatherResult.weather_date") as "weatherDate")
      .agg(avg("weatherResult.temperature") as "averageTemperature")
      .where("averageTemperature > 0.00")

    val kafkaDf = resDf.select(col("geoHash") as "key",
      to_json(struct($"weatherDate", $"geoHash", $"averageTemperature"))
        .as("value"))

    kafkaDf
      .writeStream
      .format("kafka")
      .option("checkpointLocation", "/tmp/tim/checkpoint7")
      .option("kafka.bootstrap.servers", PropertyReader.getProperty(Settings.kafkaUrl))
      .option("topic", PropertyReader.getProperty(Settings.kafkaAggregateWeatherTopic))
      .outputMode(OutputMode.Complete())
      .start()
  }
}
