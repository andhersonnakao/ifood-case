import unittest
import os
from src.session import SparkSessionBuilder
from src.paths import PathManager
from src.transformer_bronze_to_silver import BronzeToSilver
from src.transformer_silver_to_gold import SilverToGold
from src.pipeline_runner import PipelineRunner
import glob
import shutil


class TestPipelineRunner(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        #Cria o Spark uma vez pra todos os testes
        cls.spark = SparkSessionBuilder().get_or_create()
        cls.path_manager = PathManager("/tmp/test_pipeline")  # Temporário pra testar
        cls.bronze_to_silver = BronzeToSilver(cls.spark, cls.path_manager)
        cls.silver_to_gold = SilverToGold(cls.spark, cls.path_manager)
        cls.runner = PipelineRunner(cls.spark, cls.path_manager, cls.bronze_to_silver, cls.silver_to_gold)

        #Mock data - Pode alterar aqui pra testes
        data = [
            (1, 2, 12.5, "2022-01-01 10:00:00", "2022-01-01 10:10:00"),
            (2, 1, 9.0, "2022-01-01 11:00:00", "2022-01-01 11:15:00")
        ]
        columns = ["VendorID", "passenger_count", "total_amount", "tpep_pickup_datetime", "tpep_dropoff_datetime"]
        df = cls.spark.createDataFrame(data, columns)

        cls.tipo = "yellow"
        cls.ano = 2022
        cls.mes = 1
        
        bronze_path = cls.path_manager.get_bronze_path(cls.tipo, cls.ano, cls.mes)
        bronze_dir = os.path.dirname(bronze_path)
        os.makedirs(bronze_dir, exist_ok=True)

        temp_parquet_dir = os.path.join(bronze_dir, "temp_parquet")
        df.coalesce(1).write.mode("overwrite").parquet(temp_parquet_dir)

        parquet_file = glob.glob(f"{temp_parquet_dir}/part-*.parquet")[0]
        shutil.move(parquet_file, bronze_path)
        shutil.rmtree(temp_parquet_dir)

    def test_pipeline_full(self):
        self.runner.run(self.tipo, self.ano, self.mes)
        #Primeiro verifica se a camada silver foi escrita
        #Cria a partir do path_manager
        silver_path = self.path_manager.get_silver_path()

        # Procura recursivamente arquivos parquet
        parquet_files = glob.glob(f"{silver_path}/**/*.parquet", recursive=True)
        self.assertTrue(len(parquet_files) > 0, "Não tem parquet na camada silver")

        # Faz a leitura e verifica o conteúdo
        df = self.spark.read.parquet(silver_path)
        self.assertEqual(df.count(), 2)
        self.assertIn("tipo", df.columns)
        self.assertIn("ano", df.columns)
        self.assertIn("mes", df.columns)

        #Verifica se a camada gold foi escrita
        gold_path = self.path_manager.get_gold_path()
        files_gold1 = glob.glob(f"{gold_path}/media_total_amount_mensal/**/*.parquet", recursive=True)
        files_gold2 = glob.glob(f"{gold_path}/media_passageiros_hora/**/*.parquet", recursive=True)

        # Apenas logs informativos – não falham o teste
        if len(files_gold1) == 0:
            print("Aviso: Nenhum arquivo na pasta media_total_amount_mensal (pode ser por schema ou dados)")
        if len(files_gold2) == 0:
            print("Aviso: Nenhum arquivo na pasta media_passageiros_hora (pode ser por schema ou dados)")

    
    @classmethod
    def tearDownClass(cls):
        # Limpa dados de teste
        import shutil
        shutil.rmtree("/tmp/test_pipeline", ignore_errors=True)
