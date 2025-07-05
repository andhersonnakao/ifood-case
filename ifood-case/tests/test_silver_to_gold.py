import unittest
from src.session import SparkSessionBuilder
from src.transformer_silver_to_gold import SilverToGold
from src.paths import PathManager


class TestSilverToGold(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSessionBuilder().get_or_create()
        cls.pm = PathManager("/tmp/test")
        cls.silver2gold = SilverToGold(cls.spark, cls.pm)
        data = [(1, 2, 12.5, "2023-05-01 10:00:00", "2023-05-01 10:10:00", "yellow", 2023, 5)]
        cols = ["VendorID", "passenger_count", "total_amount", "pickup_datetime", "dropoff_datetime", "tipo", "ano", "mes"]
        cls.df = cls.spark.createDataFrame(data, cols)

    def test_generate_gold_tables(self):
        df1, df2 = self.silver2gold.generate_gold_tables(self.df)
        self.assertTrue(df1.count() >= 1)
        self.assertTrue(df2.count() >= 1)