import unittest
from src.paths import PathManager


class TestPathManager(unittest.TestCase):
    def test_bronze_path(self):
        pm = PathManager("/tmp/test")
        self.assertEqual(pm.get_bronze_path("yellow", 2023, 1), "/tmp/test/bronze/yellow_tripdata_2023-01.parquet")

    def test_silver_path(self):
        pm = PathManager("/tmp/test")
        self.assertEqual(pm.get_silver_path(), "/tmp/test/silver")

    def test_gold_path(self):
        pm = PathManager("/tmp/test")
        self.assertEqual(pm.get_gold_path(), "/tmp/test/gold")