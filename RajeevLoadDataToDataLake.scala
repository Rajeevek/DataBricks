// Databricks notebook source
// Load the dataframe as CSV to data lake
greenTaxiTripDataDF  
    .write
    .option("header", "true")
    .option("dateFormat", "yyyy-MM-dd HH:mm:ss.S")
    .mode(SaveMode.Overwrite)
    .csv("/mnt/csvFiles/DimensionalModel/Facts/GreenTaxiFact.csv")