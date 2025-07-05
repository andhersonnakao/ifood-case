from pyspark.sql.functions import lit, col
from pyspark.sql.types import *
import os

#Classe pra criação de transformação de bronze pra silver
class BronzeToSilver:
    #Chama a sessão Spark
    def __init__(self, spark, path_manager):
        self.spark = spark
        self.paths = path_manager
        
    #Lê os arquivos no diretório
    def read_files(self, path):
        if not os.path.isfile(path):
            raise Exception(f"Arquivo não encontrado: {path}")

        try:
            df = self.spark.read.format("parquet").load(path)
            return df
        except Exception as e:
            raise Exception(f"Erro ao ler arquivos em {path}: {e}")

    #Seleciona somente as colunas necessárias
    def clean_schema(self, df, tipo):
        if tipo == "yellow":
            return df.selectExpr(
                "cast(VendorID as long) as VendorID",
                "cast(passenger_count as long) as passenger_count",
                "cast(total_amount as double) as total_amount",
                "cast(tpep_pickup_datetime as timestamp) as pickup_datetime",
                "cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime"
            )
        elif tipo == "green":
            return df.selectExpr(
                "cast(VendorID as long) as VendorID",
                "cast(passenger_count as long) as passenger_count",
                "cast(total_amount as double) as total_amount",
                "cast(lpep_pickup_datetime as timestamp) as pickup_datetime",
                "cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime"
            )
        else:
            raise Exception(f"Tipo não suportado: {tipo}")
        
    #Adiciona as colunas tipo, ano e mês pra considerar nas partições
    def add_partition_columns(self, df, tipo, ano, mes):
        return df.withColumn("tipo", lit(tipo)) \
                 .withColumn("ano", lit(ano)) \
                 .withColumn("mes", lit(mes))

    #Escreve na camada silver os dados pré-tratados
    def write_silver(self, df, tipo, ano, mes, output_path):
        df = self.add_partition_columns(df, tipo, ano, mes)
        df.write.mode('overwrite')\
                .option("partitionOverwriteMode", "dynamic") \
                .partitionBy('tipo', 'ano', 'mes')\
                .parquet(output_path)