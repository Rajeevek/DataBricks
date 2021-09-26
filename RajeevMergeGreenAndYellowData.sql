-- Databricks notebook source
--Merge data
SELECT 'Green' AS TaxiType
      , PickupTime
      , DropTime
      , PickupLocationId
      , DropLocationId      
      , TripTimeInMinutes
      , TripType
FROM global_temp.FactGreenTaxiTripData

UNION

SELECT 'Yellow' AS TaxiType
      , PickupTime
      , DropTime
      , PickupLocationId
      , DropLocationId
      , TripTimeInMinutes
      , TripType
FROM global_temp.FactYellowTaxiTripData



-- COMMAND ----------

-- MAGIC %scala
-- MAGIC // Extract Taxi Zones data
-- MAGIC var taxiZonesDF = spark
-- MAGIC                     .read
-- MAGIC                     .option("header", "true")
-- MAGIC                     .option("inferSchema", "true")
-- MAGIC                     .csv("/mnt/csvFiles/TaxiZones.csv")
-- MAGIC 
-- MAGIC taxiZonesDF.createOrReplaceTempView("TaxiZones") 

-- COMMAND ----------


SELECT Borough, TaxiType, COUNT(*) AS TotalSharedTrips
FROM TaxiZones
LEFT JOIN
(

    SELECT 'Green' AS TaxiType, PickupLocationId FROM global_temp.FactGreenTaxiTripData WHERE TripType = 'SharedTrip'    
    UNION ALL
    SELECT 'Yellow' AS TaxiType, PickupLocationId FROM global_temp.FactYellowTaxiTripData WHERE TripType = 'SharedTrip'    
    
) AllTaxis
ON AllTaxis.PickupLocationId = TaxiZones.LocationID

GROUP BY Borough, TaxiType
ORDER BY Borough, TaxiType