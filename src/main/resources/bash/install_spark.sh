wget http://apache.mirror.serversaustralia.com.au/spark/spark-3.0.1/spark-3.0.1-bin-hadoop3.2.tgz

tar -xvzf spark-3.0.1-bin-hadoop3.2.tgz -C ~/hadoop

nano ~/.bashrc
#export SPARK_HOME=~/hadoop/spark-3.0.1-bin-hadoop3.2
#export PATH=$SPARK_HOME/bin:$PATH
source  ~/.bashrc

#Hive suppoert
cp $HADOOP_HOME/etc/hadoop/core-site.xml $SPARK_HOME/conf/
cp $HADOOP_HOME/etc/hadoop/hdfs-site.xml $SPARK_HOME/conf/
cp $HIVE_HOME/conf/hive-site.xml $SPARK_HOME/conf/

cp spark-defaults.conf.template spark-defaults.conf

$SPARK_HOME/sbin/start-history-server.sh