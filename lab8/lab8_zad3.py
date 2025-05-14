# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.functions import col,length
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC **1. Sprawdzanie czy zmienne są null (ustawia domyślnie na 0)**

# COMMAND ----------

def is_null(df: DataFrame, columns: list) -> DataFrame:
    null_columns = {}
    for column in columns:
        null_count = df.select(column).filter(col(column).isNull()).count()
        if null_count > 0:
            print(f"Kolumna '{column}' zawiera {null_count} null.")
            null_columns[column] = 0
    
    if null_columns:
        df = df.fillna(null_columns)
    
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC **2. Sprawdzanie czy zmienne mają odpowiedni format np. liczbowy**

# COMMAND ----------

def data_types(df):
   df = df.withColumn("age", when(col("age").cast("int").isNull(), lit(0)).otherwise(col("age").cast("int"))) # zle -> 0
   df = df.withColumn("name", when(col("name").isNull() | (length(col("name")) == 0), lit("Unknown")).otherwise(co("name")))  # zmienna kategoryczna
   df = df.withColumn("salary",when(col("salary").cast("double").isNull() | (col("salary") <= 0), lit(0.0)).otherwise(col("salary").cast("double"))) # zle -> 0
   return df

    

# COMMAND ----------

# MAGIC %md
# MAGIC **3. Puste pola na domyślną wartość**

# COMMAND ----------

default_date = "1900-01-01"

def fill_missing_dates(df, date_column):
    df = df.withColumn(date_column,
        when(col(date_column).isNull() | (col(date_column) == ""), to_date(lit(default_date))).otherwise(to_date(col(date_column))))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC **4. Weryfikacja - np. liczby muszą być w konkrentym przedziale, lub mieć tylko konkretne zmienne kategoryczne**

# COMMAND ----------

min_age =0
max_age = 110
statuses = ["Child", "Married", "Not married"]
default_status = "Not married"

def validate_age_with_fallback(df: DataFrame) -> DataFrame:
    df = df.withColumn("age", F.when(F.col("age") < min_age, min_age).when(F.col("age") > max_age, max_age).otherwise(F.col("age")))

    df = df.withColumn("status",F.when(F.col("status").isin(statuses), F.col("status")).otherwise(default_status))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC **5. Zamiast pustych stringów, domyślna wartość**

# COMMAND ----------

def empty_strings(df: DataFrame, columns: list, default_value: str = "Unknown") -> DataFrame:
    for col_name in columns:
        df = df.withColumn(col_name,
            F.when((F.col(col_name) == "") | F.col(col_name).isNull(), default_value).otherwise(F.col(col_name)))
    return df