from pyspark.sql import SparkSession

#Classe pra criação da sessão Spark
class SparkSessionBuilder:
    def __init__(self, app_name: str = "ifood"):
        self.app_name = app_name

    def get_or_create(self):
        spark = SparkSession.builder.appName(self.app_name).master("local").getOrCreate()
        spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
        return spark