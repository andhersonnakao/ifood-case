# Dockerfile
FROM jupyter/pyspark-notebook:latest

USER root

RUN pip install --no-cache-dir \
    --trusted-host pypi.org --trusted-host files.pythonhosted.org \
    pandas \
    pyspark \
    findspark \
    ipykernel \
    pytest

RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    echo "JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> /etc/environment && \
    echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> ~/.bashrc

# Cria estrutura esperada no container
RUN mkdir -p /app/stream_data

# Copia os arquivos do projeto para dentro do container
COPY . /app/stream_data/

# Define o diretório de trabalho
WORKDIR /app/stream_data/notebooks

# Corrige permissões para notebooks funcionarem no Jupyter
RUN chmod -R 777 /app/stream_data
RUN chmod -R 777 /app/stream_data/notebooks

# Garante que o Python encontre os módulos internos (src/)
ENV PYTHONPATH=/app/stream_data/checkpoints/ifood