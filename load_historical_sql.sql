-- Databricks notebook source
CREATE OR REFRESH STREAMING LIVE TABLE jw_historical_hoops
COMMENT "Ingest ncaa player_game csv data"
TBLPROPERTIES ("quality" = "bronze")
AS SELECT 
 *
FROM (
  SELECT * FROM cloud_files(
    "/Volumes/tabular/dataexpert/jw_capstone/clean_csv/", 
    "csv", 
    map("header", "true", "inferSchema", "true")
  )
)
