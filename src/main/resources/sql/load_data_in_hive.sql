create database spark;
use spark;

CREATE EXTERNAL TABLE hotel (Id BIGINT,Name VARCHAR(64),Country VARCHAR(64),City VARCHAR(64),Address VARCHAR(64),
                             Latitude DOUBLE,Longitude DOUBLE)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    location 'hdfs://localhost:9000/user/spark/hotels_unzip';


CREATE EXTERNAL TABLE expedia(notused INT)
  ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  WITH SERDEPROPERTIES (
    'avro.schema.url'='file:///home/tim/schema.avsc')
  STORED as INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
    location 'hdfs://localhost:9000/user/spark/expedia';


CREATE EXTERNAL TABLE weather (lng DOUBLE,lat DOUBLE,avg_tmpr_f DOUBLE,avg_tmpr_c DOUBLE,wthr_date STRING)
 STORED AS PARQUET;

load data inpath 'hdfs://localhost:9000/user/spark/weather/*/*/*/*.parquet' into table weather;
---------------------Optimize tables----------------------------------------
create external table weather_opt(lng DOUBLE,lat DOUBLE, avg_tmpr_c DOUBLE) PARTITIONED BY (wthr_date VARCHAR(64))
    LOCATION 'hdfs://localhost:9000/home/user/spark/weather_opt';

insert overwrite table weather_opt PARTITION (wthr_date) select lng, lat, avg_tmpr_c, wthr_date from weather;