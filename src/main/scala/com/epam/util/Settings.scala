package com.epam.util

import com.epam.util

object Settings extends Enumeration {
  type Settings = Value

  val kafkaStartOffset: util.Settings.Value = Value("kafka_start_offset")
  val appName: util.Settings.Value = Value("app_name")
  val kafkaUrl: util.Settings.Value = Value("kafka_url")
  val hotelTopic: util.Settings.Value = Value("kafka_hotel_topic")
  val weatherTopic: util.Settings.Value = Value("weather_enriched_topic")
  val kafkaAggregateWeatherTopic: util.Settings.Value = Value("aggregate_weather_topic")
  val hotelWeatherExpediaHdfsUrl: util.Settings.Value = Value("hotel_weather_expedia_hdfs_url")
  val expediaHdfsUrl: util.Settings.Value = Value("expedia_hdfs_url")
  val cleanExpediaHdfsUrl: util.Settings.Value = Value("clean_expedia_hdfs_url")
  val hotelWeatherJoinUrl: util.Settings.Value = Value("hotel_weather_join_url")
  val expedia17JoinWeatherHotelUrl: util.Settings.Value = Value("expedia17_hotel_weather_join_url")
  val initialStateUrl: util.Settings.Value = Value("initial_state_url")
  val resultStateUrl: util.Settings.Value = Value("result_state_url")
}
