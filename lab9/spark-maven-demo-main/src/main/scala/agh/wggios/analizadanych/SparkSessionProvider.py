from pyspark.sql import SparkSession
from LoggingUtils import setup_logging


class SparkSessionProvider:
    def __init__(self):
        setup_logging()
        self.spark = SparkSession.builder \
            .appName("jakas_nazwa") \
            .config("spark.driver.memory", "2000m") \
            .master("local[4]") \
            .getOrCreate()

    def get_spark_session(self):
        return self.spark
