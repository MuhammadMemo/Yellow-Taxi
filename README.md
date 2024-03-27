  ## >    🚕 Yellow Taxi 
     📕 Data Dictionary - Trip Records 

  *  ️⃣ VendorID A code indicating the LPEP provider that provided the record.    
  1= Creative Mobile Technologies, LLC; 2= VeriFone Inc. 

  * ⏰ lpep_pickup_datetime The date and time when the meter was engaged.  
  * ⏰ lpep_dropoff_datetime The date and time when the meter was disengaged.   
  
  * 🧑‍🤝‍🧑 Passenger_count The number of passengers in the vehicle.    **This is a driver-entered value.**
  
  * 📌 Trip_distance The elapsed trip distance in **miles** reported by the taximeter. 
  
  * 📍 PULocationID TLC Taxi Zone in which the taximeter was **engaged**
  
  * 📍 DOLocationID TLC Taxi Zone in which the taximeter was **disengaged**
   
  * 💹 RateCodeID The final rate code in effect at the end of the trip.  
  1= Standard rate 
  2=JFK 
  3=Newark 
  4=Nassau or Westchester 
  5=Negotiated fare 
  6=Group ride 
  
  * ✳ Store_and_fwd_flag This flag indicates whether the trip record was held in vehicle 
  memory before sending to the vendor, aka “store and forward,” 
  because the vehicle did not have a connection to the server.   
  Y= store and forward trip 
  N= not a store and forward trip 
  
  * 💳 Payment_type A numeric code signifying how the passenger paid for the trip.  
  1= Credit card 
  2= Cash 
  3= No charge 
  4= Dispute 
  5= Unknown 
  6= Voided trip 
  
  * 💰 Fare_amount The time-and-distance fare calculated by the meter. 
  
  * 💰 Extra Miscellaneous extras and surcharges.  Currently, this only includes 
  the $0.50 and $1 rush hour and overnight charges. 
  
  * 💰 MTA_tax $0.50 MTA tax that is automatically triggered based on the metered  rate in use. 
  
  * 💰 Improvement_surcharge $0.30 improvement surcharge assessed on hailed trips at the flag drop. The improvement surcharge began being levied in 2015. 
  
  * 💰 Tip_amount Tip amount – This field is automatically populated for credit card  tips. **Cash tips are not included.**
  
  * 💰 Tolls_amount Total amount of all tolls paid in trip.  
  
  * 💰 Total_amount The total amount charged to passengers. **Does not include cash tips.** 
  
  * 🗂 Trip_type A code indicating whether the trip was a street-hail or a dispatch 
  that is automatically assigned based on the metered rate in use but 
  can be altered by the driver.   
  1= Street-hail 
  2= Dispatch
#   📓 ATTENTION!
  
  On 05/13/2022, we are making the following changes to trip record files:
  
  All files will be stored in the PARQUET format. Please see the ‘Working With PARQUET Format’ under the Data Dictionaries and MetaData section.
  Trip data will be published monthly (with two months delay) instead of bi-annually.
  HVFHV files will now include 17 more columns (please see High Volume FHV Trips Dictionary for details). Additional columns will be added to the old files as well. The earliest date to include additional columns: February 2019.
  Yellow trip data will now include 1 additional column (‘airport_fee’, please see Yellow Trips Dictionary for details). The additional column will be added to the old files as well. The earliest date to include the additional column: January 2011.
