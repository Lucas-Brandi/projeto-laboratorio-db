"""
Exemplo de leitura de uma tabela do Postgres via Spark (JDBC),
convertendo para pandas para as etapas seguintes do pipeline.

Uso:
    python src/spark_read_example.py

Requer o driver JDBC do PostgreSQL. Se o Spark reclamar de driver não
encontrado, baixe o .jar do postgresql-jdbc e aponte pelo --jars ao
rodar via spark-submit, ou configure spark.jars no SparkSession.
"""
import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_SSLMODE = os.environ.get("DB_SSLMODE", "require")

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode={DB_SSLMODE}"


def main():
    spark = (
        SparkSession.builder
        .appName("leitura-postgres")
        # Se necessário, descomente e ajuste o caminho do driver JDBC:
        # .config("spark.jars", "/caminho/para/postgresql-42.7.4.jar")
        .getOrCreate()
    )

    df = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "exemplo")  # troque pelo nome real da tabela
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    df.show()

    # Convertendo para pandas para as etapas seguintes do pipeline, se precisar
    pandas_df = df.toPandas()
    print(pandas_df.head())

    spark.stop()


if __name__ == "__main__":
    main()
