\\#ifood-case - Pipeline de Processamento com PySpark





Este projeto implementa um pipeline de ingestão, transformação e análise de dados de corridas de táxi, utilizando PySpark localmente com suporte a Docker.





\\##Estrutura







\\- src/: código fonte do pipeline (extração, transformações, runner)



\\- tests/: testes unitários para todas as etapas do pipeline



\\- analysis/: notebook com análise final dos dados processados



\\- docker-compose.yml / Dockerfile(s)



\\- README.md e requirements.txt



\\##Pré-requisitos

\- Python 3.8+

\- Docker e Docker Compose (opcional, caso queira rodar com Spark e Jupyter)





\\##Execução em ambiente local

1- Instale as dependências:

   pip install -r requirements.txt



2- Rode o pipeline principal:

    python3 main.py



3- Execute os testes

    python3 -m unittest discover -s tests



4- Com um notebook rodar:

    Pasta: analysis/analysis.ipynb



\\##Execução com docker

1- Suba os conteineres com Spark e Jupyter:

    docker-compose up -d



2- (Dentro do container spark\_master) Vá até /app/stream\_data e rode o pipeline principal:

    cd /app/stream\_data

    python3 main.py



4- Execute os testes

    python3 -m unittest discover -s tests



5- Acesse o conteiner Jupyter e altere as permissões da pasta caso necessário

    chmod -R 777 /app/stream\_data



6- Acesse o Jupyter Notebook:

    URL: http://localhost:8888

&nbsp;   Token: mysecuretoken

    Pasta: analysis/analysis.ipynb







Sobre os Testes:



        Testes de transformação bronze pra silver

        Testes de silver pra gold

        Testes do runner completo

