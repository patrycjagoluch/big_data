// Databricks notebook source
// MAGIC %md
// MAGIC **Zad.1**

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions._

val df = spark.read
  .option("multiline", true)
  .json("/FileStore/tables/Nested.json")

// COMMAND ----------

val newDF = df.withColumn(
  "pathLinkInfo",
  col("pathLinkInfo.elevationGain").dropFields("elevationAgainstDirection"))

// COMMAND ----------

val newDF = df.withColumn(
  "pathLinkInfo",
  col("pathLinkInfo").dropFields("endGradeSeparation", "formsPartOfPath"))

// COMMAND ----------

val newDF2 = newDF.drop("attribute1")

// COMMAND ----------

// MAGIC %md
// MAGIC 1. Pobierz dane Spark-The-Definitive_Guide dostępne na github
// MAGIC 2. Użyj danych do zadania '../retail-data/all/online-retail-dataset.csv'
// MAGIC
// MAGIC

// COMMAND ----------

// MAGIC %sh
// MAGIC wget https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/master/data/retail-data/all/online-retail-dataset.csv -O /tmp/online-retail-dataset.csv
// MAGIC

// COMMAND ----------

dbutils.fs.cp("file:/tmp/online-retail-dataset.csv", "dbfs:/FileStore/online-retail-dataset.csv")

val df = spark.read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("dbfs:/FileStore/online-retail-dataset.csv")

// COMMAND ----------

display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC 3. Zapisz DataFrame do formatu delta i stwórz dużą ilość parycji (kilkaset)
// MAGIC * Partycjonuj po Country
// MAGIC

// COMMAND ----------

val path = "dbfs:/tmp/online_retail_delta_scala"

df.repartition(400, $"Country")
  .write
  .format("delta")
  .partitionBy("Country")
  .mode("overwrite")
  .save(path)

df.repartition(150, $"Country")
  .write
  .format("delta")
  .partitionBy("Country")
  .mode("overwrite")
  .save(path)

df.repartition(50, $"Country")
  .write
  .format("delta")
  .partitionBy("Country")
  .mode("overwrite")
  .save(path)

// COMMAND ----------

val c = spark.read.format("delta").load(path).count()

// COMMAND ----------

spark.sql("DROP TABLE IF EXISTS TabelaRaw")

spark.sql(s"""
  CREATE TABLE TabelaRaw
  USING Delta
  LOCATION '$path'
""")

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1: OPTIMIZE and ZORDER
// MAGIC
// MAGIC Wykonaj optymalizację do danych stworzonych w części I `../delta/retail-data/`.
// MAGIC
// MAGIC Dane są partycjonowane po kolumnie `Country`.
// MAGIC
// MAGIC Przykładowe zapytanie dotyczy `StockCode`  = `22301`. 
// MAGIC
// MAGIC Wykonaj zapytanie i sprawdź czas wykonania. Działa szybko czy wolno 
// MAGIC
// MAGIC Zmierz czas zapytania kod poniżej - przekaż df do `sqlZorderQuery`.

// COMMAND ----------

def timeIt[T](op: => T): Float = {
 val start = System.currentTimeMillis
 val res = op
 val end = System.currentTimeMillis
 (end - start) / 1000.toFloat
}

val sqlZorderQuery = timeIt {
  spark.sql("SELECT * FROM TabelaRaw WHERE StockCode = '22301'").collect()
}

println(s"without ZORDER: $sqlZorderQuery seconds")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from TabelaRaw where StockCode = '22301'

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC Skompaktuj pliki i przesortuj po `StockCode`.

// COMMAND ----------

// MAGIC %sql
// MAGIC -- wypelnij
// MAGIC OPTIMIZE TabelaRaw
// MAGIC ZORDER by (StockCode)

// COMMAND ----------

// MAGIC %md
// MAGIC Uruchom zapytanie ponownie tym razem użyj `postZorderQuery`.

// COMMAND ----------

// TODO
val postZorderQuery = timeIt(spark.sql("SELECT * FROM TabelaRaw WHERE StockCode = '22301'").collect())

println(s"with ZORDER: $postZorderQuery seconds")

// COMMAND ----------

// MAGIC %md
// MAGIC ## 2: VACUUM
// MAGIC
// MAGIC Policz liczbę plików przed wykonaniem `VACUUM` for `Country=Sweden` lub innego kraju

// COMMAND ----------

// TODO
val SwedPath = "dbfs:/tmp/online_retail_delta_scala/Country=Sweden/"
val plikiPrzed = dbutils.fs.ls(SwedPath).length

// COMMAND ----------

// MAGIC %md
// MAGIC Teraz wykonaj `VACUUM` i sprawdź ile było plików przed i po.

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC VACUUM TabelaRaw

// COMMAND ----------

// MAGIC %md
// MAGIC Policz pliki dla wybranego kraju `Country=Sweden`.

// COMMAND ----------

// TODO
val plikiPo = dbutils.fs.ls(SwedPath).length

// COMMAND ----------

// MAGIC %md
// MAGIC ## Przeglądanie histrycznych wartośći
// MAGIC
// MAGIC możesz użyć funkcji `describe history` żeby zobaczyć jak wyglądały zmiany w tabeli. Jeśli masz nową tabelę to nie będzie w niej history, dodaj więc trochę danych żeby zoaczyć czy rzeczywiście się zmieniają. 

// COMMAND ----------

// MAGIC %sql
// MAGIC describe history TabelaRaw