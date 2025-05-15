from src.main.scala.agh.wggios.analizadanych.datareader.DataReader import DataReader
from src.main.scala.agh.wggios.analizadanych.datareader.DataProcessor import DataProcessor
from src.main.scala.agh.wggios.analizadanych.datareader.DataWriter import DataWriter
from pyspark.sql import SparkSession
from LoggingUtils import setup_logging
import logging
# import sys


def main():
    setup_logging()
    logging.info("Start aplikacji")

    spark = SparkSession.builder.appName("spark-demo").master("local[*]").getOrCreate()

    path = r"D:\6_sem\big_data\zadania\lab9\data.csv"
    output_path = r"D:\6_sem\big_data\zadania\lab9\output"

    reader = DataReader(spark)
    df = reader.read_csv(path)
    df.show()

    processor = DataProcessor()
    transformed_df = processor.transform_data(df)
    transformed_df.show()

    writer = DataWriter()
    writer.write_csv(transformed_df, output_path)

    spark.stop()


if __name__ == "__main__":
    main()
