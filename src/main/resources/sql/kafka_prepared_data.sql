
drop table if exists hotel_kafka;
drop table if exists weather_kafka;

CREATE EXTERNAL TABLE weather_kafka (temperature DOUBLE, lng DOUBLE,lat DOUBLE, weather_date STRING)
    STORED BY 'org.apache.hadoop.hive.kafka.KafkaStorageHandler'
    TBLPROPERTIES
        ("kafka.topic" = "weather_kafka",
        "kafka.bootstrap.servers"="localhost:9092");
INSERT INTO TABLE weather_kafka
SELECT avg_tmpr_c as temperature, lng, lat, wthr_date as weather_date,
       null AS `__key`, null AS `__partition`, -1 AS `__offset`, null AS `__timestamp`
FROM weather_opt where wthr_date = '2016-10-01';

CREATE EXTERNAL TABLE hotel_kafka
(name VARCHAR(64), date_checkin VARCHAR(64), date_checkout VARCHAR(64))
    STORED BY 'org.apache.hadoop.hive.kafka.KafkaStorageHandler'
    TBLPROPERTIES
        ("kafka.topic" = "hotel_kafka",
        "kafka.bootstrap.servers"="localhost:9092"
        );


INSERT INTO TABLE hotel_kafka
SELECT *,
       null AS `__key`, null AS `__partition`, -1 AS `__offset`, null AS `__timestamp`
FROM hotel limit 100;