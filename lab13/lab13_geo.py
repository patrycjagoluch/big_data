# Databricks notebook source
from mosaic import enable_mosaic
enable_mosaic(spark, dbutils)
from pyspark.sql.functions import *
import mosaic as mos

# COMMAND ----------

path = 'dbfs:/FileStore/tables/map.geojson'

# COMMAND ----------

geo_df = spark.read.format("geojson").load(path)

points_df = geo_df.filter("ST_GeometryType(geometry) = 'Point'")
polygons_df = geo_df.filter("ST_GeometryType(geometry) = 'Polygon'")

# COMMAND ----------

points_df.createOrReplaceTempView("points")
polygons_df.createOrReplaceTempView("polygons")

spark.sql("SELECT ST_Area(geometry) AS area FROM polygons").show()

# COMMAND ----------

spark.sql("""
    SELECT p.geometry AS point_geom
    FROM points p, polygons poly
    WHERE ST_Contains(poly.geometry, p.geometry)
""").show()


# COMMAND ----------

points = points_df.limit(2).collect()
if len(points) == 2:
    point1 = points[0].geometry
    point2 = points[1].geometry

    spark.sql(f"""
        SELECT ST_Distance(
            ST_GeomFromWKT('{point1.toWkt()}'),
            ST_GeomFromWKT('{point2.toWkt()}')
        ) AS distance
    """).show()

# COMMAND ----------

spark.sql("""
    SELECT p.geometry AS point_geom
    FROM points p, polygons poly
    WHERE ST_Intersects(poly.geometry, p.geometry)
""").show()

# COMMAND ----------

spark.sql("""
    SELECT p.geometry AS point_geom
    FROM points p, polygons poly
    WHERE ST_Within(p.geometry, poly.geometry)
""").show()