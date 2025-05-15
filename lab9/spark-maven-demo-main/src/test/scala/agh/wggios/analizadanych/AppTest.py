import unittest
from pyspark.sql import SparkSession


class AppTest(unittest.TestCase):
    spark: SparkSession

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("AppTest") \
            .master("local[*]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_ok(self):
        self.assertTrue(True)

    # def test_ko(self):
    #     self.assertTrue(False)


if __name__ == '__main__':
    unittest.main()
