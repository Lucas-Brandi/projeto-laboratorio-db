"""
Exemplo de leitura de uma tabela do Postgres via Spark (JDBC),
convertendo para pandas para as etapas seguintes do pipeline.

Uso:
    python src/spark_read_example.py
"""

import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession


# Carrega as variáveis do arquivo .env
load_dotenv()


# Descobre automaticamente onde está a pasta do projeto
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Caminho para o driver JDBC do PostgreSQL
JDBC_JAR = os.path.join(
    BASE_DIR,
    "jars",
    "postgresql-42.7.13.jar"
)


# Credenciais do PostgreSQL/Aiven
DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_SSLMODE = os.environ.get("DB_SSLMODE", "require")


# Endereço JDBC utilizado pelo Spark
JDBC_URL = (
    f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
    f"?sslmode={DB_SSLMODE}"
)


def main():

    # Inicia o Spark
    spark = (
        SparkSession.builder
        .appName("leitura-postgres")
        .config("spark.driver.extraClassPath", JDBC_JAR)
        .config("spark.executor.extraClassPath", JDBC_JAR)
        .getOrCreate()
    )

    # Lê a tabela sales diretamente do PostgreSQL
    df = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "sales")
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    # Exibe os dados no formato Spark
    print("\nDADOS DA TABELA SALES NO SPARK:")
    df.show()

    # Converte para Pandas
    pandas_df = df.toPandas()

    print("\nDADOS CONVERTIDOS PARA PANDAS:")
    print(pandas_df.head())

    spark.stop()


if __name__ == "__main__":
    main()