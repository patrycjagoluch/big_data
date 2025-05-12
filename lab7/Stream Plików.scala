// Databricks notebook source
// MAGIC %md ## Dane
// MAGIC Dane są dostępne na AWS i dostęp zapewnia Databricks `/databricks-datasets/structured-streaming/events/` 

// COMMAND ----------

// MAGIC %fs ls /databricks-datasets/structured-streaming/events/

// COMMAND ----------

// MAGIC %fs head /databricks-datasets/structured-streaming/events/file-0.json

// COMMAND ----------

// MAGIC %md 
// MAGIC * Stwórz osobny folder 'streamDir' do którego będziesz kopiować część plików. możesz użyć dbutils....
// MAGIC * Pozostałe pliki będziesz kopiować jak stream będzie aktywny

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.mkdirs("/FileStore/streamDir")
// MAGIC dbutils.fs.cp("/databricks-datasets/structured-streaming/events/file-0.json", "/FileStore/streamDir/file-0.json")
// MAGIC dbutils.fs.cp("/databricks-datasets/structured-streaming/events/file-1.json", "/FileStore/streamDir/file-1.json")
// MAGIC

// COMMAND ----------

// MAGIC %md ## Analiza danych/Statyczny DF
// MAGIC * Stwórz schemat danych i wyświetl zawartość danych z oginalnego folderu

// COMMAND ----------

import org.apache.spark.sql.types._

val inputPath = "/databricks-datasets/structured-streaming/events/"

val jsonSchema = new StructType()
  .add("time", TimestampType)
  .add("action", StringType)
  .add("user", StringType)
  .add("device", StringType)

val staticInputDF = spark.read
  .schema(jsonSchema)
  .json(inputPath)

display(staticInputDF)

// COMMAND ----------

// MAGIC %md 
// MAGIC Policz ilość akcji "open" i "close" w okienku (window) jedno godzinnym (kompletny folder). 

// COMMAND ----------

import org.apache.spark.sql.functions._

val iloscAkcji = staticInputDF
  .groupBy(
    window(col("time"), "1 hour"),
    col("action")
  ).count()
 

iloscAkcji.createOrReplaceTempView("akcje_godzina")
iloscAkcji.orderBy("window").show()

// COMMAND ----------

// MAGIC %md 
// MAGIC Użyj sql i pokaż na wykresie ile było akcji 'open' a ile 'close'.

// COMMAND ----------

// MAGIC %sql 
// MAGIC SELECT action, sum(count) AS total_count FROM akcje_godzina GROUP BY action

// COMMAND ----------

// MAGIC %md
// MAGIC Użyj sql i pokaż ile było akcji w każdym dniu i godzinie przykład ('Jul-26 09:00')

// COMMAND ----------

// MAGIC %sql
// MAGIC select  date_format(window.start, 'MMM-dd HH:00') AS godzina, SUM(count) AS liczba_akcji
// MAGIC from akcje_godzina group by date_format(window.start, 'MMM-dd HH:00') order by godzina

// COMMAND ----------

// MAGIC %md ## Stream Processing 
// MAGIC Teraz użyj streamu.
// MAGIC * Ponieważ będziesz streamować pliki trzeba zasymulować, że jest to normaly stream. Podpowiedź dodaj opcję 'maxFilesPerTrigger'
// MAGIC * Użyj 'streamDir' niekompletne pliki

// COMMAND ----------

import org.apache.spark.sql.functions._

//odpal stream
val streamingInputDF = 
  spark.readStream
  .schema(jsonSchema)
  .option("maxFilesPerTrigger", 1)
  .json("/FileStore/streamDir")


// sumujemy open i close tak ja jak powyżej w okienku jednogodzinnym
val streamingCountsDF = streamingInputDF
  .groupBy(
    window(col("time"), "1 hour"),
    col("action")
  )
  .count()


// COMMAND ----------

// MAGIC %md
// MAGIC Sprawdź czy stream działa

// COMMAND ----------


streamingInputDF.isStreaming

// COMMAND ----------

// MAGIC %md 
// MAGIC * Zredukuj partyce shuffle do 4 
// MAGIC * Teraz ustaw Sink i uruchom stream
// MAGIC * użyj formatu 'memory'
// MAGIC * 'outputMode' 'complete'

// COMMAND ----------


spark.conf.set("spark.sql.shuffle.partitions", "4")
val query = streamingCountsDF.writeStream
  .format("memory") 
  .outputMode("complete") 
  .queryName("akcje_stream")
  .start()


// COMMAND ----------

// MAGIC %md 
// MAGIC `query` działa teraz w tle i wczytuje pliki cały czas uaktualnia count. Postęp widać w Dashboard

// COMMAND ----------

Thread.sleep(3000) // lekkie opóźnienie żeby poczekać na wczytanie plików

// COMMAND ----------

// MAGIC %md
// MAGIC * Użyj sql żeby pokazać ilość akcji w danym dniu i godzinie 

// COMMAND ----------

// MAGIC %sql select action, date_format(window.end, "MMM-dd HH:mm") as time, count from akcje_stream  order by time, action

// COMMAND ----------

// MAGIC %md 
// MAGIC * Sumy mogą się nie zgadzać ponieważ wcześniej użyłeś niekompletnych danych.
// MAGIC * Teraz przekopiuj resztę plików z orginalnego folderu do 'streamDir', sprawdź czy widać zmiany 
// MAGIC

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.cp("/databricks-datasets/structured-streaming/events/file-2.json", "/FileStore/streamDir/file-2.json")
// MAGIC dbutils.fs.cp("/databricks-datasets/structured-streaming/events/file-3.json", "/FileStore/streamDir/file-3.json")
// MAGIC dbutils.fs.cp("/databricks-datasets/structured-streaming/events/file-4.json", "/FileStore/streamDir/file-4.json")

// COMMAND ----------

// MAGIC %sql 
// MAGIC -- użyj zapytania jak wcześniej pokazujący symy z datą i godziną powinny pasować do danych z pierwszego statycznego DF
// MAGIC select action, date_format(window.end, "MMM-dd HH:mm") as time,  SUM(count) AS total_count 
// MAGIC from akcje_stream group by action, date_format(window.end, "MMM-dd HH:mm") order by time, action

// COMMAND ----------

// MAGIC %md
// MAGIC * Zatrzymaj stream

// COMMAND ----------

query.stop()