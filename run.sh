export HADOOP_HOME=/data/yarn
export SPARK_HOME=/data/spark
export HADOOP_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)
java -cp $HADOOP_CLASSPATH:$SPARK_HOME/lib/*:./target/spark-sql-yarn-0.1-SNAPSHOT-jar-with-dependencies.jar org.apache.spark.sql.client.JavaSparkSQLClient ./spark-defaults.conf hdfs://namenode:9000/user/temp/ "select 1+2"
