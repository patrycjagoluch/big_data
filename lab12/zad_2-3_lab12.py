# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

df = spark.read.option("header", True).csv("/databricks-datasets/airlines/part-00000")
df = df.withColumn("Year", col("Year").cast("int"))
df.show(5)

# COMMAND ----------

df.write.mode("overwrite").partitionBy("Year").parquet("/tmp/loty_partitioned")

display(dbutils.fs.ls("/tmp/loty_partitioned"))

# COMMAND ----------

spark.conf.set("spark.sql.sources.default", "parquet")
df.write.mode("overwrite").bucketBy(5, "Year").sortBy("Year").saveAsTable("loty_bucketed")

display(dbutils.fs.ls("dbfs:/user/hive/warehouse/loty_bucketed"))

# COMMAND ----------

# MAGIC %md
# MAGIC **ZAD.3**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE school_db;
# MAGIC USE school_db;
# MAGIC
# MAGIC CREATE TABLE teachers (name STRING, teacher_id INT);
# MAGIC INSERT INTO teachers VALUES ('Tom', 1), ('Jerry', 2);
# MAGIC
# MAGIC CREATE TABLE students (name STRING, student_id INT) PARTITIONED BY (student_id);
# MAGIC INSERT INTO students VALUES ('Mark', 111111), ('John', 222222);
# MAGIC
# MAGIC ANALYZE TABLE students COMPUTE STATISTICS NOSCAN;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED students;

# COMMAND ----------

# MAGIC %sql
# MAGIC ANALYZE TABLE students COMPUTE STATISTICS;
# MAGIC DESC EXTENDED students;

# COMMAND ----------

# MAGIC %sql
# MAGIC ANALYZE TABLE students PARTITION (student_id = 111111) COMPUTE STATISTICS;
# MAGIC
# MAGIC DESC EXTENDED students PARTITION (student_id = 111111);

# COMMAND ----------

# MAGIC %sql
# MAGIC ANALYZE TABLE students COMPUTE STATISTICS FOR COLUMNS name;
# MAGIC
# MAGIC DESC EXTENDED students name;

# COMMAND ----------

# MAGIC %sql
# MAGIC ANALYZE TABLES IN school_db COMPUTE STATISTICS NOSCAN;
# MAGIC
# MAGIC DESC EXTENDED teachers;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED students;

# COMMAND ----------

# MAGIC %sql
# MAGIC ANALYZE TABLES COMPUTE STATISTICS;
# MAGIC
# MAGIC DESC EXTENDED teachers;