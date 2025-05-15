from pyspark.sql import DataFrame
import logging


class DataReader:
    def __init__(self, spark):
        self.spark = spark

    @staticmethod
    def log_info(message: str):
        logging.basicConfig(level=logging.INFO)
        logging.info(message)

    def read_csv(self, path: str) -> DataFrame:
        self.log_info("CZYTAM SOBIE PLIK")
        return self.spark.read.format("csv") \
            .option("header", True) \
            .option("inferSchema", True) \
            .load(path)
