from src.session import SparkSessionBuilder
from src.pipeline_runner import PipelineRunner
from src.paths import PathManager
from src.transformer_bronze_to_silver import BronzeToSilver
from src.transformer_silver_to_gold import SilverToGold
from src.session import SparkSessionBuilder
from src.extractor import DataExtractorTaxi


if __name__ == "__main__":
    spark = SparkSessionBuilder("ifood-pipeline").get_or_create()
    path_manager = PathManager()
    bronze2silver = BronzeToSilver(spark, path_manager)
    silver2gold = SilverToGold(spark, path_manager)
    pipeline = PipelineRunner(spark, path_manager, bronze2silver, silver2gold)

    extractor = DataExtractorTaxi() 

    #Lista com os tipos, ano e mês
    tipos = ["yellow", "green"]
    anos = [2023]
    meses = [1, 2, 3, 4, 5]

    # Executa o pipeline para cada combinação
    for tipo in tipos:
        for ano in anos:
            for mes in meses:
                try:
                    extractor.download_file(tipo, ano, mes)
                except Exception as e:
                    print(f"Erro ao processar {tipo} {ano}-{mes:02d}: {e}")

    # Executa o pipeline para cada combinação
    for tipo in tipos:
        for ano in anos:
            for mes in meses:
                try:
                    pipeline.run(tipo, ano, mes)
                except Exception as e:
                    print(f"Erro ao processar {tipo} {ano}-{mes:02d}: {e}")


    try:
        pipeline.run_silver_to_gold()
    except Exception as e:
        print(f"Erro na transformação silver->gold: {e}")

