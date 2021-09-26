// Databricks notebook source
//Yellow Taxi clean up operations
val defaultValueMap = Map(
                            "payment_type" -> 5,
                            "RatecodeID" -> 1
                         )

var yellowTaxiTripDataDF = spark
                              .read
                              .option("header", "true")
                              .option("inferSchema", "true")
                              .csv("/mnt/csvFiles/yellow_tripdata_2018-12.csv")

yellowTaxiTripDataDF = yellowTaxiTripDataDF
                              .where("passenger_count > 0")
                              .filter($"trip_distance" > 0.0)

                              .na.drop(Seq("PULocationID", "DOLocationID"))

                              .na.fill(defaultValueMap)

                              .dropDuplicates()

                              .where("tpep_pickup_datetime >= '2018-12-01' AND tpep_dropoff_datetime < '2019-01-01'")

// Display the count after filtering
println("After filters = " + yellowTaxiTripDataDF.count())

// COMMAND ----------

//Yellow Taxi transformations
import org.apache.spark.sql.functions._

// Apply transformations to Yellow taxi data
yellowTaxiTripDataDF = yellowTaxiTripDataDF

                        // Select only limited columns
                        .select(
                                  $"VendorID",
                                  $"passenger_count".alias("PassengerCount"),
                                  $"trip_distance".alias("TripDistance"),
                                  $"tpep_pickup_datetime".alias("PickupTime"),                          
                                  $"tpep_dropoff_datetime".alias("DropTime"), 
                                  $"PUlocationID".alias("PickupLocationId"), 
                                  $"DOlocationID".alias("DropLocationId"), 
                                  $"RatecodeID", 
                                  $"total_amount".alias("TotalAmount"),
                                  $"payment_type".alias("PaymentType")
                               )

                        // Create derived columns for year, month and day
                        .withColumn("TripYear", year($"PickupTime"))
                        .withColumn("TripMonth", month($"PickupTime"))
                        .withColumn("TripDay", dayofmonth($"PickupTime"))
                        
                        // Create a derived column - Trip time in minutes
                        .withColumn("TripTimeInMinutes", 
                                        round(
                                                (unix_timestamp($"DropTime") - unix_timestamp($"PickupTime")) 
                                                    / 60
                                             )
                                   )

                        // Create a derived column - Trip type, and drop SR_Flag column
                        .withColumn("TripType", 
                                        when(
                                                $"RatecodeID" === 6,
                                                  "SharedTrip"
                                            )
                                        .otherwise("SoloTrip")
                                   )
                        .drop("RatecodeID")
display(yellowTaxiTripDataDF)

// COMMAND ----------

// Green Taxi clean up operations
val defaultValueMap = Map(
                            "payment_type" -> 5,
                            "RatecodeID" -> 1
                         )

var greenTaxiTripDataDF = spark
                              .read
                              .option("header", "true")
                              .option("inferSchema", "true")                              
                              .option("delimiter", "\t")    
                              .csv("/mnt/csvFiles/GreenTaxiTripData_201812.csv")

greenTaxiTripDataDF = greenTaxiTripDataDF
                              .where("passenger_count > 0")
                              .filter($"trip_distance" > 0.0)

                              .na.drop(Seq("PULocationID", "DOLocationID"))

                              .na.fill(defaultValueMap)

                              .dropDuplicates()

                              .where("lpep_pickup_datetime >= '2018-12-01' AND lpep_dropoff_datetime < '2019-01-01'")

// Display the count after filtering
println("After filters = " + greenTaxiTripDataDF.count())

// COMMAND ----------

//Green Taxi transformations

import org.apache.spark.sql.functions._

// Apply transformations to Green taxi data
greenTaxiTripDataDF = greenTaxiTripDataDF

                        // Select only limited columns
                        .select(
                                  $"VendorID",
                                  $"passenger_count".alias("PassengerCount"),
                                  $"trip_distance".alias("TripDistance"),
                                  $"lpep_pickup_datetime".alias("PickupTime"),                          
                                  $"lpep_dropoff_datetime".alias("DropTime"), 
                                  $"PUlocationID".alias("PickupLocationId"), 
                                  $"DOlocationID".alias("DropLocationId"), 
                                  $"RatecodeID", 
                                  $"total_amount".alias("TotalAmount"),
                                  $"payment_type".alias("PaymentType")
                               )

                        // Create derived columns for year, month and day
                        .withColumn("TripYear", year($"PickupTime"))
                        .withColumn("TripMonth", month($"PickupTime"))
                        .withColumn("TripDay", dayofmonth($"PickupTime"))
                        
                        // Create a derived column - Trip time in minutes
                        .withColumn("TripTimeInMinutes", 
                                        round(
                                                (unix_timestamp($"DropTime") - unix_timestamp($"PickupTime")) 
                                                    / 60
                                             )
                                   )

                        // Create a derived column - Trip type, and drop SR_Flag column
                        .withColumn("TripType", 
                                        when(
                                                $"RatecodeID" === 6,
                                                  "SharedTrip"
                                            )
                                        .otherwise("SoloTrip")
                                   )
                        .drop("RatecodeID")

display(greenTaxiTripDataDF)

// COMMAND ----------

//Create Temp Views for Yellow and Green Taxi Data
yellowTaxiTripDataDF.createOrReplaceGlobalTempView("FactYellowTaxiTripData")

greenTaxiTripDataDF.createOrReplaceGlobalTempView("FactGreenTaxiTripData")


// COMMAND ----------

// MAGIC %sql
// MAGIC --Read the data from the temp views
// MAGIC SELECT 'Green' AS TaxiType
// MAGIC       , PickupTime
// MAGIC       , DropTime
// MAGIC       , PickupLocationId
// MAGIC       , DropLocationId      
// MAGIC       , TripTimeInMinutes
// MAGIC       , TripType
// MAGIC FROM global_temp.FactGreenTaxiTripDataDF

// COMMAND ----------

// Load the dataframe as CSV to data lake
greenTaxiTripDataDF  
    .write
    .option("header", "true")
    .option("dateFormat", "yyyy-MM-dd HH:mm:ss.S")
    .mode(SaveMode.Overwrite)
    .csv("/mnt/csvFiles/DimensionalModel/Facts/GreenTaxiFact.csv")

// COMMAND ----------

// Check the number of partitions on the dataframe
greenTaxiTripDataDF
    .rdd
    .getNumPartitions

// COMMAND ----------

// Decrease the number of partitions on dataframe to 1
greenTaxiTripDataDF = greenTaxiTripDataDF  
                        .coalesce(10)

// COMMAND ----------

// Load the dataframe to new partition
greenTaxiTripDataDF  
    .write
    .option("header", "true")
    .option("dateFormat", "yyyy-MM-dd HH:mm:ss.S")
    .mode(SaveMode.Overwrite)
    .csv("/mnt/csvFiles/DimensionalModel/Facts/GreenTaxiFact.csv")

// COMMAND ----------

// Read the stored CSV file
val greenTaxiCsvDF = spark
                        .read
                        .option("header", "true")
                        .csv("/mnt/csvFiles/DimensionalModel/Facts/GreenTaxiFact.csv")                        

greenTaxiCsvDF.select("PickupLocationId", "DropLocationId").distinct().count()

// COMMAND ----------

// Load the dataframe in parquet Format
greenTaxiTripDataDF  
    .write
    .option("header", "true")
    .option("dateFormat", "yyyy-MM-dd HH:mm:ss.S")
    .mode(SaveMode.Overwrite)
    .parquet("/mnt/csvFiles/DimensionalModel/Facts/GreenTaxiFact.parquet")

// COMMAND ----------

// Read the stored Parquet file
val greenTaxiParquetDF = spark
                            .read                            
                            .parquet("/mnt/csvFiles/DimensionalModel/Facts/GreenTaxiFact.parquet")                            

greenTaxiParquetDF.select("PickupLocationId", "DropLocationId").distinct().count()

// COMMAND ----------

