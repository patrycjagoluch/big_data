// Databricks notebook source
// MAGIC %fs ls  /FileStore/tables/brzydki.json

// COMMAND ----------

val df = spark.read.format("json")
.option("multiline","true")
.load("dbfs:/FileStore/tables/brzydki.json")
.selectExpr(
  "jobDetails",
  "maiaExtractProcessDetails",
  "numberOfFeatures",
  "explode(features) as feature")


// COMMAND ----------

display(df)

// COMMAND ----------

df.printSchema()

// COMMAND ----------

df.select("feature").printSchema()

// COMMAND ----------

import org.apache.spark.sql.functions._

val flattened = df.select(
  "feature.geometry.type",
  "feature.properties.accessTopologyComponent",
  "feature.properties.administrativeExceptionComponent",
  "feature.properties.administrativeUnitComponent",
  "feature.properties.anomalyComponent",
  "feature.properties.areaHeightComponent",
  "feature.properties.baseFormComponent.componentId",
  "feature.properties.geometricZoneComponent",
  "feature.properties.changeEventType",
  "feature.properties.boundaryLinkComponent"
)

// COMMAND ----------

flattened.printSchema()