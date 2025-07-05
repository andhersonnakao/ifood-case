import unittest
from src.session import SparkSessionBuilder
from src.transformer_bronze_to_silver import BronzeToSilver
from src.paths import PathManager


class TestBronzeToSilver(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSessionBuilder().get_or_create()
        cls.pm = PathManager("/tmp/test")
        cls.transformer = BronzeToSilver(cls.spark, cls.pm)
        data = [(1, 2, 12.5, "2023-01-01 10:00:00", "2023-01-01 10:10:00")]
        cols = ["VendorID", "passenger_count", "total_amount", "tpep_pickup_datetime", "tpep_dropoff_datetime"]
        cls.df = cls.spark.createDataFrame(data, cols)

    def test_clean_schema(self):
        df_clean = self.transformer.clean_schema(self.df, tipo="yellow")
        self.assertIn("VendorID", df_clean.columns)