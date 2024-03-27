
show databases;
--DROP DATABASE taxi_db CASCADE;
create database  if not exists Taxi_db;
describe database taxi_db;

use Taxi_db;
select current_database();
-- show file warehouse
--set spark.sql.warehouse.dir; 
/* 
CREATE TEMPORARY VIEW taxi_stg
USING org.apache.spark.sql.parquet
OPTIONS (
  path "/home/hadoop/mydata/yellow_tripdata_2022-02.parquet"
) */

Create table if not exists taxi_stg
(
VendorID                bigint ,
 tpep_pickup_datetime   timestamp_ntz ,
tpep_dropoff_datetime   timestamp_ntz,
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
USING parquet
OPTIONS (path='/home/hadoop/mydata/yellow_tripdata_${dateM}.parquet');

--${dateM}
REFRESH table taxi_stg;

--drop table taxi_stg;
--describe taxi_stg;
0000

/*The t able is supposed to be external table.
Whether you specify external or not, unless and until you specify the path.
You don't need to explicitly specify external.
In this case, when we run this, it will take care of creating the external table in the database.
*/ 
----------------------------------------------------------------------------------
Create table if not exists Taxi_Years
(
VendorID                bigint ,
pickup_date             date ,
pickup_time             string,
dropoff_date            date,
dropoff_time            string ,
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
USING delta
partitioned  by (TaxiMonth int);
-----------------------------------
--INSERT OVERWRITE TABLE Taxi_Years PARTITION (TaxiMonth)
INSERT into TABLE Taxi_Years PARTITION (TaxiMonth)
SELECT 
VendorID,
to_date(tpep_pickup_datetime,'yyyy-MM-dd') as pickup_date,
date_format(tpep_pickup_datetime, 'HH:mm:ss') as pickup_time,
to_date(tpep_dropoff_datetime,'yyyy-MM-dd') as dropoff_date,
date_format(tpep_dropoff_datetime, 'HH:mm:ss') as dropoff_time,
passenger_count,
trip_distance,
RatecodeID,
store_and_fwd_flag,
PULocationID,
DOLocationID,
payment_type,
fare_amount,
extra,
mta_tax,
tip_amount,
tolls_amount,
improvement_surcharge,
total_amount,
congestion_surcharge,
airport_fee,
CAST(date_format(tpep_pickup_datetime, "yyyyMM") as int) as TaxiMonth
 FROM taxi_stg ;

select * from Taxi_Years limit 10;
select count(*) from Taxi_Years;
--2463931
----------------------------------------------------------------------
drop table taxi_stg;

--------------------------------------------------------------------
--SHOW PARTITIONS taxi_db.Taxi_Years;
--DESCRIBE TABLE EXTENDED Taxi_Years;
--DESCRIBE TABLE  Taxi_Years;


--DESCRIBE DETAIL Taxi_Years;

--!hdfs dfs -ls /home/hadoop/spark-warehouse/taxi_db.db/taxi_years;

--hadoop@ubuntu:~$ ls /home/hadoop/spark-warehouse/taxi_db.db/taxi_years
--drop table Taxi_Years;
-----------------------------------------------------------------------------------
Create table if not exists payment_type(ID int,Pay_Name string)
USING csv
OPTIONS(path = '/home/hadoop/mydata/MastrData/Payment_type.csv');
select * from payment_type limit 10;

-----------------------------------------------------------------------------------
Create Table if not exists Taxi_location(LocationID int,Borough string,Zone string ,service_zone string)
USING csv
OPTIONS (path= '/home/hadoop/mydata/MastrData/taxi_location.csv');
select * from Taxi_location limit 10;


show functions

-- COMMAND ----------

describe function substring

-- COMMAND ----------

describe function trim
