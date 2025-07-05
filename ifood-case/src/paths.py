import os

#Definição dos diretórios que serão utilizados
class PathManager:
    def __init__(self, base_dir="/app/stream_data/checkpoints/ifood/"):
        self.base = base_dir
	
	#Criação do diretório caso não exista
        os.makedirs(os.path.join(self.base, "bronze"), exist_ok=True)
        os.makedirs(self.get_silver_path(), exist_ok=True)
        os.makedirs(self.get_gold_path(), exist_ok=True)

    def get_bronze_path(self, tipo, ano, mes):
        nome_arquivo = f"{tipo}_tripdata_{ano}-{int(mes):02d}.parquet"
        return os.path.join(self.base, "bronze", nome_arquivo)

    def get_silver_path(self):
        return f"{self.base}/silver"

    def get_gold_path(self):
        return f"{self.base}/gold"