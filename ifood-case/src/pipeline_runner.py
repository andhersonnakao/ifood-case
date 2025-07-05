#Rodar o pipeline inteiro
class PipelineRunner:
    def __init__(self, spark, path_manager, transformer, silver_to_gold):
        self.spark = spark
        self.transformer = transformer
        self.paths = path_manager
        self.silver_to_gold = silver_to_gold
    
    def run(self, tipo, ano, mes):
        print(f"Iniciando pipeline para {tipo} - {ano}-{mes:02d}")

        path_bronze = self.paths.get_bronze_path(tipo, ano, mes)
        df_bronze = self.transformer.read_files(path_bronze)
        df_clean = self.transformer.clean_schema(df_bronze, tipo)

        path_silver = self.paths.get_silver_path()
        self.transformer.write_silver(df_clean, tipo, ano, mes, path_silver)

        print("Transformação bronze to silver finalizada.")

    def run_silver_to_gold(self):
        print("Iniciando transformação silver to gold em lote.")
        path_silver = self.paths.get_silver_path()
        df_silver = self.spark.read.parquet(path_silver)

        df_gold1, df_gold2 = self.silver_to_gold.generate_gold_tables(df_silver)
        self.silver_to_gold.save_gold_table(df_gold1, df_gold2)

        print("Transformação silver to gold finalizada.")