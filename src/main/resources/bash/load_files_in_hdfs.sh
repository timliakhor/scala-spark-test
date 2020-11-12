hadoop fs -mkdir 'hdfs://localhost:9000/user/spark'
hdfs dfs -copyFromLocal /home/tim/hotels_unzip hdfs://localhost:9000/user/spark

hdfs dfs -copyFromLocal /home/tim/schema.avsc hdfs://localhost:9000/user/spark/schema.avsc
hdfs dfs -copyFromLocal /home/tim/expedia hdfs://localhost:9000/user/spark
hdfs dfs -copyFromLocal /home/tim/weather hdfs://localhost:9000/user/spark