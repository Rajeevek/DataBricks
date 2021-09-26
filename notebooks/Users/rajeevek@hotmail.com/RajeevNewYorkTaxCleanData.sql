-- Databricks notebook source
-- MAGIC %scala
-- MAGIC // Apply schema to FHV taxi data
-- MAGIC var fhvTaxiTripDataDF = spark.read.option("header", "true").csv("dbfs:/mnt/csvFiles/fhv_tripdata_2018-12.csv")
-- MAGIC display(fhvTaxiTripDataDF)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC // Clean and filter the FHV taxi data
-- MAGIC import org.apache.spark.sql.functions._
-- MAGIC 
-- MAGIC fhvTaxiTripDataDF = fhvTaxiTripDataDF.na.drop(Seq("PULocationID", "DOLocationID")).dropDuplicates().where("Pickup_DateTime >= '2018-12-01' AND Dropoff_DateTime < '2019-01-01'")
-- MAGIC // Select only limited columns
-- MAGIC fhvTaxiTripDataDF = fhvTaxiTripDataDF
-- MAGIC                         .select(
-- MAGIC                                   "Pickup_DateTime", 
-- MAGIC                                   "DropOff_DateTime", 
-- MAGIC                                   "PUlocationID", 
-- MAGIC                                   "DOlocationID", 
-- MAGIC                                   "SR_Flag", 
-- MAGIC                                   "Dispatching_base_number"
-- MAGIC                                )                        
-- MAGIC 
-- MAGIC fhvTaxiTripDataDF.printSchema

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC // Rename the columns
-- MAGIC fhvTaxiTripDataDF = fhvTaxiTripDataDF
-- MAGIC                         .withColumnRenamed("Pickup_DateTime", "PickupTime")
-- MAGIC                         .withColumnRenamed("DropOff_DateTime", "DropTime")
-- MAGIC                         .withColumnRenamed("PUlocationID", "PickupLocationId")
-- MAGIC                         .withColumnRenamed("DOlocationID", "DropLocationId")
-- MAGIC                         .withColumnRenamed("Dispatching_base_number", "BaseLicenseNumber")
-- MAGIC 
-- MAGIC display(fhvTaxiTripDataDF)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC // Create a derived column - Trip time in minutes
-- MAGIC fhvTaxiTripDataDF = fhvTaxiTripDataDF
-- MAGIC                         .withColumn("TripTimeInMinutes", 
-- MAGIC                                         round(
-- MAGIC                                                 (unix_timestamp($"DropTime") - unix_timestamp($"PickupTime")) 
-- MAGIC                                                     / 60
-- MAGIC                                              )
-- MAGIC                                    )                                               
-- MAGIC 
-- MAGIC display(fhvTaxiTripDataDF)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC // Read FHV Bases json file
-- MAGIC var fhvBasesDF = spark
-- MAGIC   .read
-- MAGIC   .option("multiline", "true")
-- MAGIC   .json("/mnt/csvFiles/FhvBases.json")
-- MAGIC 
-- MAGIC display(fhvBasesDF)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC // Create schema for FHV Bases
-- MAGIC import org.apache.spark.sql.types.StructType
-- MAGIC import org.apache.spark.sql.types._
-- MAGIC 
-- MAGIC val fhvBasesSchema = StructType(
-- MAGIC   List(
-- MAGIC     StructField("License Number", StringType, true),
-- MAGIC     StructField("Entity Name", StringType, true),
-- MAGIC     StructField("Telephone Number", LongType, true),
-- MAGIC     StructField("SHL Endorsed", StringType, true),
-- MAGIC     StructField("Type of Base", StringType, true),
-- MAGIC     
-- MAGIC     StructField("Address", 
-- MAGIC                 StructType(List(
-- MAGIC                     StructField("Building", StringType, true),
-- MAGIC                     StructField("Street", StringType, true), 
-- MAGIC                     StructField("City", StringType, true), 
-- MAGIC                     StructField("State", StringType, true), 
-- MAGIC                     StructField("Postcode", StringType, true))),
-- MAGIC                 true
-- MAGIC                ),
-- MAGIC                 
-- MAGIC     StructField("GeoLocation", 
-- MAGIC                 StructType(List(
-- MAGIC                     StructField("Latitude", StringType, true),
-- MAGIC                     StructField("Longitude", StringType, true), 
-- MAGIC                     StructField("Location", StringType, true))),
-- MAGIC                 true
-- MAGIC               )   
-- MAGIC   )
-- MAGIC )
-- MAGIC 
-- MAGIC // Apply schema to FHV bases
-- MAGIC var fhvBasesDF = spark.read
-- MAGIC   .schema(fhvBasesSchema)
-- MAGIC   .option("multiline", "true")
-- MAGIC   .json("/mnt/csvFiles/FhvBases.json")
-- MAGIC 
-- MAGIC display(fhvBasesDF)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC var fhvBasesFlatDF = fhvBasesDF
-- MAGIC                         .select(
-- MAGIC                                   $"License Number".alias("BaseLicenseNumber"),                          
-- MAGIC                                   $"Type of Base".alias("BaseType"),
-- MAGIC 
-- MAGIC                                   $"Address.Building".alias("AddressBuilding"),
-- MAGIC                                   $"Address.Street".alias("AddressStreet"),
-- MAGIC                                   $"Address.City".alias("AddressCity"),
-- MAGIC                                   $"Address.State".alias("AddressState"),
-- MAGIC                                   $"Address.Postcode".alias("AddressPostCode")
-- MAGIC                                )
-- MAGIC 
-- MAGIC display(fhvBasesFlatDF)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC // Create schema for FHV Bases
-- MAGIC import org.apache.spark.sql.types.StructType
-- MAGIC import org.apache.spark.sql.types._
-- MAGIC // Create a dataframe joining FHV trip data with bases
-- MAGIC var fhvTaxiTripDataWithBasesDF = fhvTaxiTripDataDF
-- MAGIC                                      .join(fhvBasesFlatDF,                                               
-- MAGIC                                                Seq("BaseLicenseNumber"),
-- MAGIC                                               "inner"
-- MAGIC                                           )
-- MAGIC 
-- MAGIC display(fhvTaxiTripDataWithBasesDF)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC // Create a dataframe for report
-- MAGIC var fhvTaxiTripReportDF = fhvTaxiTripDataWithBasesDF
-- MAGIC                               .groupBy("AddressCity", "BaseType")
-- MAGIC                               .agg(sum("TripTimeInMinutes"))
-- MAGIC 
-- MAGIC                               .withColumnRenamed("sum(TripTimeInMinutes)", "TotalTripTime")
-- MAGIC                               .orderBy("AddressCity", "BaseType")
-- MAGIC 
-- MAGIC display(fhvTaxiTripReportDF)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC // Create local temp view - that can be used only in this notebook
-- MAGIC fhvTaxiTripDataWithBasesDF.createOrReplaceTempView("LocalFhvTaxiTripData")
-- MAGIC fhvTaxiTripDataWithBasesDF.createOrReplaceGlobalTempView("FactFhvTaxiTripData")
-- MAGIC fhvTaxiTripReportDF.createOrReplaceTempView("LocalfhvTaxiTripReportDF")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC // Use spark.sql to run a SQL query
-- MAGIC var sqlBasedDF = spark.sql("SELECT * FROM LocalFhvTaxiTripData WHERE BaseLicenseNumber = 'B02510'")
-- MAGIC 
-- MAGIC display(sqlBasedDF.limit(10))

-- COMMAND ----------

SELECT * 
FROM LocalFhvTaxiTripData
WHERE BaseLicenseNumber = 'B02510'
LIMIT 10

-- COMMAND ----------

Select * from LocalfhvTaxiTripReportDF