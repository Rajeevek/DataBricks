# Databricks notebook source
# MAGIC %scala
# MAGIC spark.conf.set("fs.azure.account.auth.type.rajeevdatalakestorage.dfs.core.windows.net", "OAuth")
# MAGIC spark.conf.set("fs.azure.account.oauth.provider.type.rajeevdatalakestorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.id.rajeevdatalakestorage.dfs.core.windows.net", "ApplicationID")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.secret.rajeevdatalakestorage.dfs.core.windows.net", dbutils.secrets.get(scope="NewYorkTaxiKeyVault",key="ClientSecret"))
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.endpoint.rajeevdatalakestorage.dfs.core.windows.net", "https://login.microsoftonline.com/TenantID/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("mnt/csvFiles"))

# COMMAND ----------

dbutils.fs.head("mnt/csvFiles/TaxiZones.csv")

# COMMAND ----------

yellowTaxiTripData = spark.read.option("header","true").csv("dbfs:/mnt/csvFiles/yellow_tripdata_2018-12.csv")
display(yellowTaxiTripData)

# COMMAND ----------

greenTaxiTripData = spark.read.option("header","true").option("delimeter","\t").csv("dbfs:/mnt/csvFiles/GreenTaxiTripData_201812.csv")
display(yellowTaxiTripData)

# COMMAND ----------



# COMMAND ----------

paymentTypeDF =  spark.read.json("dbfs:/mnt/csvFiles/PaymentTypes.json")
display(paymentTypeDF)

# COMMAND ----------

# MAGIC %scala
# MAGIC // Create schema for FHV taxi data
# MAGIC import org.apache.spark.sql.types.StructType
# MAGIC import org.apache.spark.sql.types._
# MAGIC 
# MAGIC val fhvTaxiTripSchema = StructType(
# MAGIC                     List(
# MAGIC                           StructField("Pickup_DateTime", TimestampType, true),
# MAGIC                           StructField("DropOff_datetime", TimestampType, true),
# MAGIC                           StructField("PUlocationID", IntegerType, true),
# MAGIC                           StructField("DOlocationID", IntegerType, true),
# MAGIC                           StructField("SR_Flag", IntegerType, true),
# MAGIC                           StructField("Dispatching_base_number", StringType, true),
# MAGIC                           StructField("Dispatching_base_num", StringType, true)
# MAGIC                     )
# MAGIC              ) 
# MAGIC val fhvTaxiTripData = spark.read.schema(fhvTaxiTripSchema).csv("dbfs:/mnt/csvFiles/fhv_tripdata_2018-12.csv")
# MAGIC display(fhvTaxiTripData)

# COMMAND ----------



# COMMAND ----------

# MAGIC %fs head /mnt/csvFiles/FhvBases.json

# COMMAND ----------

# MAGIC %scala
# MAGIC // Read FHV Bases json file
# MAGIC var fhvBasesDF = spark
# MAGIC   .read
# MAGIC   .option("multiline", "true")
# MAGIC   .json("/mnt/csvFiles/FhvBases.json")
# MAGIC 
# MAGIC display(fhvBasesDF)

# COMMAND ----------

# MAGIC %scala
# MAGIC // Create schema for FHV Bases
# MAGIC import org.apache.spark.sql.types.StructType
# MAGIC import org.apache.spark.sql.types._
# MAGIC 
# MAGIC val fhvBasesSchema = StructType(
# MAGIC   List(
# MAGIC     StructField("License Number", StringType, true),
# MAGIC     StructField("Entity Name", StringType, true),
# MAGIC     StructField("Telephone Number", LongType, true),
# MAGIC     StructField("SHL Endorsed", StringType, true),
# MAGIC     StructField("Type of Base", StringType, true),
# MAGIC     
# MAGIC     StructField("Address", 
# MAGIC                 StructType(List(
# MAGIC                     StructField("Building", StringType, true),
# MAGIC                     StructField("Street", StringType, true), 
# MAGIC                     StructField("City", StringType, true), 
# MAGIC                     StructField("State", StringType, true), 
# MAGIC                     StructField("Postcode", StringType, true))),
# MAGIC                 true
# MAGIC                ),
# MAGIC                 
# MAGIC     StructField("GeoLocation", 
# MAGIC                 StructType(List(
# MAGIC                     StructField("Latitude", StringType, true),
# MAGIC                     StructField("Longitude", StringType, true), 
# MAGIC                     StructField("Location", StringType, true))),
# MAGIC                 true
# MAGIC               )   
# MAGIC   )
# MAGIC )
# MAGIC 
# MAGIC 
# MAGIC // Apply schema to FHV bases
# MAGIC var fhvBasesDF = spark.read
# MAGIC   .schema(fhvBasesSchema)
# MAGIC   .option("multiline", "true")
# MAGIC   .json("/mnt/csvFiles/FhvBases.json")
# MAGIC 
# MAGIC display(fhvBasesDF)