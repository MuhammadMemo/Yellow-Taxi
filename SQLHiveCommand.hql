
--beeline -u jdbc:hive2://localhost:10000/default  -f /home/hadoop/SparkProjects/Taix/SQLHiveCommand.hql
--drop database taxi_db cascade;

SET hive.execution.engine=spark;
SET spark.yarn.queue = prod;
SET spark.master=yarn;
SET spark.app.name=HiveTaxi;
--set spark.executor.memory=512m;             
--Set spark.serialize r= org.apache.spark.serializer.KryoSerializer;

--set spark.executor.memory=2g ; 
--set spark.driver.memory=1g;
--SET spark.driver.cores = 5;
--set spark.yarn.executor.memoryOverhead=15;
-- SET spark.driver.memory = 5g;
-- SET spark.executor.memory=5g;
-- SET spark.submit.deployMode=cluster;
--set hive.spark.client.server.connect.timeout=3000000ms;
--Set hive.exec.parallel=true;
--In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=5;
--In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=5;
--In order to set a constant number of reducers:
  set mapreduce.job.reduces=5;
SET hive.auto.convert.join=false;
--SET hive.cli.print.header=true;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 1000000000;
SET hive.exec.max.dynamic.partitions.pernode = 100000000;
--SET mapreduce.map.memory.mb=16192;
--SET mapreduce.reduce.memory.mb=16192;
--set hive.msck.repair.batch.size=1;
--set hive.msck.path.validation=ignore;
set mapred.reduce.tasks = 38;
--set hive.exec.reducers.max=1
--INSERT into TABLE Taxi_Years partition (TaxiMonth)

use Taxi_db;
INSERT OVERWRITE TABLE Taxi_Years partition (TaxiMonth)
select  
*  ,cast(date_format(from_unixtime(cast(tpep_dropoff_datetime as BIGINT) DIV 1000000) , "yyyyMM") as int) as TaxiMonth       
from taxi_stg limit 1000000; 
--
select count(*) from Taxi_Years; 

show databases;

create database  if not exists Taxi_db;
--describe database taxi_db;


select current_database();

----------------------------------------------------------------------
--# Create table  taxi_stg
Create table if not exists taxi_stg
(
VendorID                bigint ,
tpep_pickup_datetime    string ,
tpep_dropoff_datetime   string,
passenger_count         double ,
trip_distance           double ,
RatecodeID              double ,
store_and_fwd_flag      string , 
PULocationID            bigint ,
DOLocationID            bigint ,
payment_type            bigint ,
fare_amount             double ,
extra                   double ,
mta_tax                 double ,
tip_amount              double ,
tolls_amount            double ,
improvement_surcharge   double ,
total_amount            double ,
congestion_surcharge    double, 
airport_fee             double 
)
COMMENT 'A table to store taxi_stg records.'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED as parquet;
--Location 'user/hive/Lake/yellow_tripdata_2022-01.parquet' ;

--# Load DATA
LOAD DATA INPATH '/user/hive/Lake/yellow_tripdata_2022-01.parquet' OVERWRITE into table taxi_stg;
describe formatted taxi_stg;

select 
from_unixtime(cast(tpep_pickup_datetime as BIGINT) DIV 1000000) as tpep_pickup_datetime,
from_unixtime(cast(tpep_dropoff_datetime as BIGINT) DIV 1000000) as tpep_dropoff_datetime 
from taxi_stg limit 10 ;
--drop table taxi_stg;
---------------------------------------------------------------------------------------------------------
--# Create table Taxi_Years
Create table if not exists Taxi_Years
(
vendorid                bigint,
tpep_pickup_datetime    string ,
tpep_dropoff_datetime   string ,
passenger_count         double,
trip_distance           double ,
ratecodeid              double ,
store_and_fwd_flag      string ,
pulocationid            bigint ,
dolocationid            bigint ,
payment_type            bigint ,
fare_amount             double ,
extra                   double ,
mta_tax                 double ,
tip_amount              double,
tolls_amount            double ,
improvement_surcharge   double ,
total_amount            double ,
congestion_surcharge    double ,
airport_fee             double
)
PARTITIONED by (TaxiMonth int)
STORED as parquet;
--# INSERT into TABLE Taxi_Years

INSERT into TABLE Taxi_Years partition (TaxiMonth)
select  
*  ,cast(date_format(from_unixtime(cast(tpep_dropoff_datetime as BIGINT) DIV 1000000) , "yyyyMM") as int) as TaxiMonth       
from taxi_stg limit 10; 

describe formatted Taxi_Years;

select *,
from_unixtime(cast(tpep_pickup_datetime as BIGINT) DIV 1000000) as tpep_pickup_datetime,
from_unixtime(cast(tpep_dropoff_datetime as BIGINT) DIV 1000000) as tpep_dropoff_datetime 
from taxi_stg limit 10 ;

select count(*) from Taxi_Years;

-- drop table Taxi_Years;
-------------------------------------------------
Create table if not exists payment_type(ID int,Pay_Name string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ;

LOAD DATA INPATH '/user/hive/Lake/Payment_type.csv' OVERWRITE into table payment_type;

select * from payment_type limit 10;
select count(*) from payment_type;

--drop table payment_type;
----------------------------------------------------------------
/* 

set hive.exec.dynamic.partition=true;
SET hive.cli.print.header=true;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 1000000;
SET hive.exec.max.dynamic.partitions.pernode = 100000;
SET mapreduce.map.memory.mb=16192;
SET mapreduce.reduce.memory.mb=16192;

#This property is not needed if you are using Hive 2.x or later
set hive.enforce.bucketing = true;

In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>

-----------------------------------------------------------------------
select cast(tpep_pickup_datetime as Timestamp) from taxi_stg limit 5;
select tpep_pickup_datetime from taxi_stg limit 10 ;
select VendorID, from_utc_timestamp(tpep_pickup_datetime,'PST') from taxi_stg limit 10 ;
SELECT COALESCE(to_date(tpep_pickup_datetime), 'N/A') AS formatted_date from taxi_stg limit 10 ;
SELECT date_format(tpep_pickup_datetime, 'dd-MM-yyyy') AS formatted_date from taxi_stg limit 10 ;
select from_unixtime(unix_timestamp(tpep_pickup_datetime ,'dd-MMM-yyyy'), 'dd-MM-yyyy') from taxi_stg limit 10 ;
--select VendorID, from_utc_timestamp(substr(trim(tpep_pickup_datetime),1,10),'PST') from taxi_stg limit 10 ;
--select VendorID, cast(to_datefrom_utc_timestamp(substr(trim(tpep_pickup_datetime),1,10),'PST')) from taxi_stg limit 10 ;

select to_date(regexp_replace(tpep_pickup_datetime,"'",""))as pickup_date from taxi_stg limit 10;
select VendorID, to_utc_timestamp(tpep_pickup_datetime,'PST') as tpep_pickup_datetime from taxi_stg limit 10;
select VendorID, to_utc_timestamp(substr(trim(tpep_pickup_datetime),1,10),'PST') as tpep_pickup_datetime from taxi_stg limit 10;
select from_unixtime(unix_timestamp(tpep_pickup_datetime,"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),'yyyy-MM-dd HH:mm:ss') as ADMIT_DATE
from taxi_stg limit 10;
select from_unixtime(unix_timestamp(tpep_pickup_datetime,"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),'yyyy-MM-dd HH:mm:ss') as ADMIT_DATE
from taxi_stg limit 10;
select VendorID, from_unixtime(unix_timestamp(to_date(tpep_pickup_datetime), 'YYYY-MM-DD HH:mm:ss')) from taxi_stg limit 10;
select cast(from_unixtime(unix_timestamp(tpep_pickup_datetime,'MM-dd-yyyy HH:mm a'), 'yyyy-dd-MM HH:mm:ss')) date_with_am from taxi_stg
limit 10;
SELECT VendorID, from_unixtime(unix_timestamp(tpep_pickup_datetime, 'dd-MM-yyyy HH:mm:ss')) FROM taxi_stg LIMIT 10;
select VendorID, from_unixtime(unix_timestamp(tpep_pickup_datetime, 'dd-MM-yyyy HH:mm')) from taxi_stg limit 10;
select VendorID, cast(tpep_pickup_datetime as string) as tpep_pickup_datetime from taxi_stg limit 10;

--select VendorID, cast(to_date(from_unixtime(unix_timestamp(tpep_pickup_datetime, 'dd-MM-yyyy'))) as date) from taxi_stg limit 10 ;
--select from_unixtime(unix_timestamp(tpep_pickup_datetime ,'dd-MMM-yyyy'), 'dd-MM-yyyy') from taxi_stg limit 10 ;
--select  from_unixtime(cast(tpep_pickup_datetime as BIGINT) DIV 1000000) as sd from taxi_stg;   */
/* 

Hive Bucketing is a way to split the table into a managed number of clusters with or without partitions. With partitions, Hive divides(creates a directory) the table into smaller parts for every distinct value of a column whereas with bucketing you can specify the number of buckets to create at the time of creating a Hive table.

n my previous article, I have explained Hive Partitions with Examples, in this article let’s learn Hive Bucketing with Examples, the advantages of using bucketing, limitations, and how bucketing works.

What is Hive Bucketing
Hive Bucketing a.k.a (Clustering) is a technique to split the data into more manageable files, (By specifying the number of buckets to create). The value of the bucketing column will be hashed by a user-defined number into buckets.

Bucketing can be created on just one column, you can also create bucketing on a partitioned table to further split the data to improve the query performance of the partitioned table.

Each bucket is stored as a file within the table’s directory or the partitions directories on HDFS.

Records with the same value in a column will always be stored in the same bucket.

Hive bucketing commonly created in two scenarios.

Create a bucket on top of the Partitioned table to further divide the table for better query performance.
Create Bucketing on the table where you cannot choose the partition column due to (too many distinct values on columns).
In our example below, I will be explaining the first approach where I create Bucketing on top of the partitioned table.

Hive Bucketing Advantages
Before jumping into the Advantages of Hive bucketing, first let’s see the limitation of Partition, with the partition you cannot control the number of partitions as it creates a partition for every distinct value of the partitioned column; which ideally creates a subdirectory for each partition inside the table directory on HDFS.

If you have too many distinct values on the partitioned column, you will end up with too many directories on HDFS which leads to higher maintenance for Name Node.

-----

Now let’s see the Advantages of Bucketing

Hive Bucketing overcomes creating too many directories by specifying the number of buckets you wanted to create (you are in control).
On a larger table, creating a bucketing gives you 2-3x better query performance than a non-bucket table.
Hive Create Bucketing Table
To create a Hive table with bucketing, use CLUSTERED BY clause with the column name you wanted to bucket and the count of the buckets.

In this article, I will explain what is Hive Partitioning and Bucketing, the difference between Hive Partitioning vs Bucketing by exploring the advantages and disadvantages of each features with examples.


At a high level, Hive Partition is a way to split the large table into smaller tables based on the values of a column(one partition for each distinct values) whereas Bucket is a technique to divide the data in a manageable form (you can specify how many buckets you want).

There are advantages and disadvantages of Partition vs Bucket so you need to choose these based on your data size and the types of Hive queries you run, let’s see the differences in detail.


Hive Partitioning vs Bucketing
Both Partitioning and Bucketing in Hive are used to improve performance by eliminating table scans when dealing with a large set of data on a Hadoop file system (HDFS). The major difference between Partitioning vs Bucketing lives in the way how they split the data.

Hive Partition is a way to organize large tables into smaller logical tables based on values of columns; one logical table (partition) for each distinct value. In Hive, tables are created as a directory on HDFS. A table can have one or more partitions that correspond to a sub-directory for each partition inside a table directory.


let’s assume you have a US census table which contains zip code, city, state and other columns. Creating a partition on state splits the table into around 50 partitions, when searching for a zipcode with in a state (state=’CA’ and zipCode =’92704′) results in faster as it need to scan only in a state=CA partition directory.

When creating partitions you have to be very cautious with the number of partitions it creates, as having too many partitions creates too many sub-directories in a table directory which bring unnecessarily and overhead to NameNode since it must keep all metadata for the file system in memory.

Hive Bucketing a.k.a (Clustering) is a technique to split the data into more manageable files, (By specifying the number of buckets to create). The value of the bucketing column will be hashed by a user-defined number into buckets.
Conclusion
In this Hive Partitioning vs Bucketing article, you have learned how to improve the performance of the queries by doing Partition and Bucket on Hive tables. These two approaches split the table into defined partitions and/or buckets, which distributes the data into smaller and more manageable parts. This eliminates table scans when you performing queries on partition and bucket columns.
 */