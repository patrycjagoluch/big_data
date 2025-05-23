# Databricks notebook source
# MAGIC %md
# MAGIC ## RDD

# COMMAND ----------

tuples = [('A', 7), ('A', 8), ('A', -4),
          ('B', 3), ('B', 9), ('B', -1),
          ('C', 1), ('C', 5)]
rdd = spark.sparkContext.parallelize(tuples)


# COMMAND ----------

# zadania wybierz wartości z tuple większe od zera wstaw brakujący kod <....>
positives = rdd.filter(lambda x: x[1] > 0)
positives.collect()

# COMMAND ----------

# policz sumę wartościdla poszczególnych kluczy (A, B, C) - wstaw brakujący kod <....>

sum_and_avg = positives.groupByKey().mapValues(lambda v: sum(v))
sum_and_avg.collect()

# COMMAND ----------


# 1. (suma, count) per key
sum_count = positives.mapValues(lambda v: (v, 1)) \
                     .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
sum_count.collect()


# COMMAND ----------


# 2. aggregate (sum, count) per key
sum_count_agg = sum_count.reduceByKey(lambda x, y:())
sum_count_agg.collect()

# COMMAND ----------

# zmapuj i wylicz średnią na każdy klucz (A, B, C) używająć funkcji mapValues - wstaw brakujący kod <....>

sum_and_avg = sum_count_agg.mapValues(lambda v: (v[0], v[0] / v[1]))
sum_and_avg.collect()

# COMMAND ----------

 data = [ 
    ("fox", 6), ("dog", 5), ("fox", 3), ("dog", 8),
    ("cat", 1), ("cat", 2), ("cat", 3), ("cat", 4)
]

ListaRrdd = sc.parallelize(data) 
ListaRrdd.collect()

# COMMAND ----------

# Uzyw funkcji reduceByKey żeby wykonać sumę wartości dla każdego klucza 
sum_per_key = ListaRrdd.reduceByKey(lambda x, y: x + y)
sum_per_key.collect()

# COMMAND ----------

# Usuń wartości większe niż 4
sum_filtered = sum_per_key.filter(lambda kv: kv[1] <= 4)
sum_filtered.collect()