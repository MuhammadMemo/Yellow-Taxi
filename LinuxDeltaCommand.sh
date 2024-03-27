 # 558. Copy Files into HDFS for Taxi Converter
#  cd hadoop-3.3.6/bin
#  # Check path in HDFS
# ./hdfs dfs  -ls /user/Data/Taxi_date
#  # Create path in HDFS
# ./hdfs dfs -mkdir -p /user/Data/Taxi_date
# ####################################################
#  # 558. Copy Files into HDFS for NYSE Converter
# ./hdfs dfs -put /home/hadoop/mydata/yellow_tripdata_2022-01.parquet /user/Data/Taxi_date
#  #Check Data  in HDFS path
# hdfs dfs  -du -h /user/Data/Taxi_date
# #$ Run Session spark sql and delta and Execute SQL File
dateM=${1}
spark-3.4.1-bin-hadoop3/bin/spark-sql \
--master yarn --queue prod --name query_app \
--conf spark.sql.warehouse.dir=/home/`whoami`/spark-warehouse \
--packages io.delta:delta-core_2.12:2.4.0 \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"  \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
-f /home/hadoop/SparkProjects/Taix/SQLDeltaCommand.sql \
-d dateM=${dateM} \
--verbose \
2>/dev/null
# Delete Files from HDFS
#hdfs dfs -rm -R -skipTrash /user/Data


spark-sql \
--master yarn --queue prod --name query_app \
--conf spark.sql.warehouse.dir=/home/`whoami`/spark-warehouse \
--packages io.delta:delta-core_2.12:2.4.0 \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"  \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
--verbose \
2>/dev/null
