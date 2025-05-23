// Databricks notebook source
case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: BigInt) 


// COMMAND ----------

val flightsDF = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("dbfs:/FileStore/tables/2010_summary.csv")

// COMMAND ----------

val flightsDS = flightsDF.as[Flight]


// COMMAND ----------

display(flightsDS.select($"DEST_COUNTRY_NAME", $"ORIGIN_COUNTRY_NAME",$"count"))

// COMMAND ----------

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import spark.implicits._
import org.apache.spark.sql.functions._
// Wykonaj mnożenie kolumny DEST_COUNTRY_NAME * string używając withColumn i zobacz co się stanie

val dfIncreased = flightsDS.withColumn("multi", col("DEST_COUNTRY_NAME") * "abc")

display(dfIncreased)

// COMMAND ----------


// Wykonaj mnożenie kolumny DEST_COUNTRY_NAME * string używając funkcji map() i zobacz co się stanie
val dsError = flightsDS.map(flight => flight.DEST_COUNTRY_NAME * "abc")
display(dsError)

// COMMAND ----------

def matchFields(row: Flight): Boolean = {
  return row.DEST_COUNTRY_NAME == row.ORIGIN_COUNTRY_NAME
}


// COMMAND ----------

// Użyj funkcji matchFields na Datafram zaobserwuj co się dzieje
flightsDF.filter(row => matchFields(row)).show()

// COMMAND ----------

// Użyj funkcji matchFields na Dataset 
flightsDS.filter(row => matchFields(row)).show()

// COMMAND ----------

case class FlightMetadata(count: BigInt ,randomData: Int)


// COMMAND ----------

val flightsMeta = spark.range(500).map(x => (x, scala.util.Random.nextInt))
.withColumnRenamed("_1","count")
.withColumnRenamed("_2","randomData").as[FlightMetadata]

display(flightsMeta)

// COMMAND ----------


// Wykonaj join pomiędzy flightsMeta i flightsDF po kolumnie 'count'
val flights2 = flightsDF.join(flightsMeta, Seq("count"))
display(flights2)

// COMMAND ----------

// MAGIC %md Joins

// COMMAND ----------


// Wykonaj joinWith pomiędzy flightsMeta i flightsDF po kolumnie 'count'
val flights3 = flightsDS.joinWith(flightsMeta, flightsDS("count") === flightsMeta("count"))
display(flights3)

// COMMAND ----------

// Wykonaj funkcję groupBy na dataset po kolumnie "DEST_COUNTRY_NAME" i zobacz jaki stworzy się obiekt
flightsDS.groupBy("DEST_COUNTRY_NAME")


// COMMAND ----------

// Podlicz ile było lotów z 'DEST_COUNTRY_NAME' używając funkcji groupByKey
val groupKey = flightsDS
  .groupByKey(flight => flight.DEST_COUNTRY_NAME)
  .count()

display(groupKey)
