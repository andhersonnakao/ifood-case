import os
import ssl
import urllib.request

#Classe pra extração de dados da fonte e inserindo em um caminho definido
class DataExtractorTaxi:
    #Aqui coloquei o destino inicial e utilizei o meu próprio storage local pelo tamanho dos arquivos
    #Poderia ter utilizado outro storage como hdfs, S3, etc, mas precisaria realizar algumas pequenas alterações no código
    def __init__(self, destino_base='/app/stream_data/checkpoints/ifood/bronze/'):
        self.destino_base = destino_base
        os.makedirs(self.destino_base, exist_ok = True)
        
    #Montar a url a partir de uma url padrão
    #Poderia também ter utilizado alguma outra forma, mas como as urls estavam padronizadas, segui com uma função em cima da url
    def url_composer(self, tipo, ano, mes):
        return f"https://d37ci6vzurychx.cloudfront.net/trip-data/{tipo}_tripdata_{ano}-{mes:02d}.parquet"

    #Função pra baixar os arquivos com urllib.request
    #Separa a string url e pega a última parte separada por "/" e usa como nome do arquivo
    def download_file(self, tipo, ano, mes):
        url = self.url_composer(tipo, ano, mes)
        file_name = url.split('/')[-1]
        destino = os.path.join(self.destino_base, file_name)
        try:
            if not os.path.exists(destino):
                ssl._create_default_https_context = ssl._create_unverified_context
                urllib.request.urlretrieve(url, destino)
            else:
                print(f"Arquivo já existe: {file_name}")
        
            return destino
        except Exception as e:
            print("Erro ao baixar {file_name}: {e}")
            return None