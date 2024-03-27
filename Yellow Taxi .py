# %% [markdown]
# >  # 🚕 Yellow Taxi 

# %% [markdown]
# ## 📕 Data Dictionary - Trip Records 

# %% [markdown]
# * #️⃣ VendorID A code indicating the LPEP provider that provided the record.    
# 1= Creative Mobile Technologies, LLC; 2= VeriFone Inc. 
# 
# * ⏰ lpep_pickup_datetime The date and time when the meter was engaged.  
# * ⏰ lpep_dropoff_datetime The date and time when the meter was disengaged.   
# 
# * 🧑‍🤝‍🧑 Passenger_count The number of passengers in the vehicle.    **This is a driver-entered value.**
# 
# * 📌 Trip_distance The elapsed trip distance in **miles** reported by the taximeter. 
# 
# * 📍 PULocationID TLC Taxi Zone in which the taximeter was **engaged**
# 
# * 📍 DOLocationID TLC Taxi Zone in which the taximeter was **disengaged**
#  
# * 💹 RateCodeID The final rate code in effect at the end of the trip.  
# 1= Standard rate 
# 2=JFK 
# 3=Newark 
# 4=Nassau or Westchester 
# 5=Negotiated fare 
# 6=Group ride 
# 
# * ✳ Store_and_fwd_flag This flag indicates whether the trip record was held in vehicle 
# memory before sending to the vendor, aka “store and forward,” 
# because the vehicle did not have a connection to the server.   
# Y= store and forward trip 
# N= not a store and forward trip 
# 
# * 💳 Payment_type A numeric code signifying how the passenger paid for the trip.  
# 1= Credit card 
# 2= Cash 
# 3= No charge 
# 4= Dispute 
# 5= Unknown 
# 6= Voided trip 
# 
# * 💰 Fare_amount The time-and-distance fare calculated by the meter. 
# 
# * 💰 Extra Miscellaneous extras and surcharges.  Currently, this only includes 
# the $0.50 and $1 rush hour and overnight charges. 
# 
# * 💰 MTA_tax $0.50 MTA tax that is automatically triggered based on the metered  rate in use. 
# 
# * 💰 Improvement_surcharge $0.30 improvement surcharge assessed on hailed trips at the flag drop. The improvement surcharge began being levied in 2015. 
# 
# * 💰 Tip_amount Tip amount – This field is automatically populated for credit card  tips. **Cash tips are not included.**
# 
# * 💰 Tolls_amount Total amount of all tolls paid in trip.  
# 
# * 💰 Total_amount The total amount charged to passengers. **Does not include cash tips.** 
# 
# * 🗂 Trip_type A code indicating whether the trip was a street-hail or a dispatch 
# that is automatically assigned based on the metered rate in use but 
# can be altered by the driver.   
# 1= Street-hail 
# 2= Dispatch
# ### 📓 ATTENTION!
# 
# On 05/13/2022, we are making the following changes to trip record files:
# 
# All files will be stored in the PARQUET format. Please see the ‘Working With PARQUET Format’ under the Data Dictionaries and MetaData section.
# Trip data will be published monthly (with two months delay) instead of bi-annually.
# HVFHV files will now include 17 more columns (please see High Volume FHV Trips Dictionary for details). Additional columns will be added to the old files as well. The earliest date to include additional columns: February 2019.
# Yellow trip data will now include 1 additional column (‘airport_fee’, please see Yellow Trips Dictionary for details). The additional column will be added to the old files as well. The earliest date to include the additional column: January 2011.

# %% [markdown]
# 

# %% [markdown]
# ## 📚 Install Libraries & Import Libraries


# %%
!pip install 'F:/30-Work/DataEngineer/03-Data/TaxiData/Lib.txt'

# %%
#!pip install 'F:/30-Work/DataEngineer/03-Data/TaxiData/Lib.txt'
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import *

from functools import reduce

# %% [markdown]
# ## 💥 Create SparkSession

# %%
spark = SparkSession.builder.appName("Taxispark").master('local').getOrCreate()
spark

# %% [markdown]
# ## 🎯 Data Injection

# %%
""" df= spark.read.format('parquet').load("F:/30-Work/DataEngineer/03-Data/TaxiData/*.parquet")
print('Total data count: ,df.count()) """

# %%
#Speerd reds
Ls = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
dfs = []
for i in Ls:
    path = f'/home/hadoop/mydata/*'
   # path = f'F:/30-Work/DataEngineer/03-Data/TaxiData/yellow_tripdata_2022-{i}.parquet'
    df = spark.read.parquet(path.format(path))
    print(path.format(i),'File Count: ', df.count())
    dfs.append(df)
    df = reduce(DataFrame.union, dfs)
print('Total Data Count: ',df.count())

# %%
dflocation=spark.read.csv("/home/hadoop/mydata/taxi_location.csv",header=True)

# %%
dfPyment=spark.read.csv("/home/hadoop/mydata/Payment_type.csv",header=True)

# %% [markdown]
# ## 📺 Data Exploratory

# %%
df.printSchema()

# %%
dflocation.printSchema()

# %%
dfPyment.printSchema()

# %%
df.show(10)

# %%
df.show(vertical=True)

# %%
dflocation.show(10,truncate=False)

# %%
dfPyment.show(10)

# %%
#df.describe().show()

# %%
# distinct count PULocationID &  payment_type
display(df.select('PULocationID').distinct().count())
display(dflocation.count())
display(df.select('payment_type').distinct().count())
display(dfPyment.count())

# %%
df.filter(df.PULocationID.isNull()).count()

# %%
df.filter(df.payment_type.isNull()).count()

# %%
df.select('payment_type').distinct().show()

# %%
df.describe(["passenger_Count"]).show()

# %%
df.filter(df.passenger_count <= 0 ).count()

# %%
df.describe(["total_amount"]).show(truncate=False,vertical=True)

# %%
### get total amount <= 0
df.filter(df.total_amount <= 0).count()

# %%
df.select(round(sum(df['tolls_amount'])).alias('Totl_Amount')).show(truncate=False)

# %% [markdown]
# ## 📐 Data Transformations

# %% [markdown]
# ### join data with payment_type

# %%
dfDataAll=df.join(dfPyment,df['payment_type']== dfPyment['id'])

# %%
dfDataAll.printSchema()

# %%
dfDataAll.tail(5)

# %% [markdown]
# ### join data with PULocationID

# %%
dfDataAll=dfDataAll.join(dflocation,dfDataAll['PULocationID'] == dflocation['LocationID'])

# %%
dfDataAll.printSchema()

# %%
dfDataAll.show(10)

# %% [markdown]
# ## ☢️ Data Clean

# %%
df=dfDataAll

# %%
# Separate date from time
df = df.withColumn("date_pickup", to_date("tpep_pickup_datetime"))
df = df.withColumn("time_pickup", date_format(col("tpep_pickup_datetime"), "HH:mm:ss"))
df = df.withColumn("date_dropoff", to_date("tpep_dropoff_datetime"))
df = df.withColumn("time_dropoff", date_format(col("tpep_dropoff_datetime"), "HH:mm:ss"))

# %%
# Add Duration_Time
df=df.withColumn('Duration_Time', (df.tpep_pickup_datetime-df.tpep_dropoff_datetime).substr(14,8))

# %%
df.show(10,truncate=False)

# %%
cols = (["tpep_pickup_datetime", "tpep_dropoff_datetime"])
df = df.drop(*cols)

# %%
df.show(10)

# %% [markdown]
# ## 📊 RFM stands for recency, frequency, monetary value Analysis 

# %%
df.select\
    (max(df.date_pickup))\
    .show()

# %%
df\
.groupBy('Borough')\
.agg(round(sum(df.total_amount),1).alias('ToTal_Amount')\
,count('Borough').alias('Count')\
,avg('ToTal_Amount').alias('avg'))\
.sort(desc('ToTal_Amount'))\
.show(truncate=False)

# %%
df.groupBy('Name')\
.agg(\
 round(sum('tolls_amount'),0).alias('ToTal_Amount')\
,count('Name').alias('Count')\
,round(avg('ToTal_Amount')).alias('avg'))\
.sort(desc('ToTal_Amount'))\
.show(truncate=False)

# %%
df\
.groupBy(date_format('date_pickup','yyyy').alias('DatePickup'))\
.agg(round(sum('tolls_amount')).alias('Amount')\
,count('date_pickup').alias('Count')\
,min('tolls_amount').alias('min')\
,max('tolls_amount').alias('max')\
,mode('tolls_amount').alias('mode')
,round(avg('tolls_amount'),2).alias('avg')).show()
#.where(col("DatePickup") == 2022)\


# %%
df.show(10)

# %% [markdown]
# ## 📇 Data Lode

# %%
!pwd


# %% [markdown]
# df.repartition(1).write.csv(
#     'abfss://MyDataLake@onelake.dfs.fabric.microsoft.com/TaxiLake.Lakehouse/Files/All/alldata', mode="overwrite", header=True)

# %%
#df.repartition(1).write.csv(
   # 'F:\\30-Work\\DataEngineer\\data', mode="overwrite", header=True)


