from pyspark.sql.functions import avg, hour

#Classe de transformação de Silver para gold
class SilverToGold:
    def __init__(self, spark, path_manager):
        self.spark = spark
        self.paths = path_manager

    def generate_gold_tables(self, df_silver):
        #Primeira tabela com a média mensal recebida
        amount_month = (df_silver\
            .groupBy("tipo", "ano", "mes")\
            .agg(avg("total_amount").alias("media_total_amount")))

        #Média de passageiros por hora
        avg_passengers_hourly = (df_silver\
            .withColumn("hora", hour("pickup_datetime"))\
            .groupBy("tipo", "ano", "mes", "hora")\
            .agg(avg("passenger_count").alias("media_passageiros")))

        return amount_month, avg_passengers_hourly
    
    def save_gold_table(self, df1, df2):
        gold_path = self.paths.get_gold_path()

        df1.write.mode("overwrite").parquet(f"{gold_path}/media_total_amount_mensal")
        df2.write.mode("overwrite").parquet(f"{gold_path}/media_passageiros_hora")