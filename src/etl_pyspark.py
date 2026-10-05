import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, when, date_format, year, quarter, month, dayofmonth, dayofweek, round

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
JDBC_JAR = BASE_DIR / "jars" / "postgresql-42.7.13.jar"
SQL_FILE = BASE_DIR / "sql" / "004_create_dw_entrega.sql"
DW_SCHEMA = "dw_techpop"

DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_SSLMODE = os.environ.get("DB_SSLMODE", "require")

JDBC_URL = (
    f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
    f"?sslmode={DB_SSLMODE}"
)


def regiao(campo):
    return (
        when(campo.isin("CA", "WA"), "WEST")
        .when(campo == "NY", "NORTHEAST")
        .when(campo.isin("TX", "FL"), "SOUTH")
        .otherwise("OTHER")
    )


def criar_dw():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        sslmode=DB_SSLMODE,
    )

    with conn:
        with conn.cursor() as cur:
            cur.execute(SQL_FILE.read_text(encoding="utf-8"))
            cur.execute(
                f"TRUNCATE TABLE {DW_SCHEMA}.fact_sales_items, "
                f"{DW_SCHEMA}.dim_product, {DW_SCHEMA}.dim_seller, "
                f"{DW_SCHEMA}.dim_customer, {DW_SCHEMA}.dim_date"
            )

    conn.close()


def ler_tabela(spark, tabela):
    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", tabela)
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def salvar(df, tabela):
    (
        df.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", f"{DW_SCHEMA}.{tabela}")
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )


def main():
    criar_dw()

    spark = (
        SparkSession.builder
        .appName("ETL-TechPop")
        .config("spark.driver.extraClassPath", str(JDBC_JAR))
        .config("spark.executor.extraClassPath", str(JDBC_JAR))
        .getOrCreate()
    )

    categories = ler_tabela(spark, "public.categories")
    customers = ler_tabela(spark, "public.customers")
    sellers = ler_tabela(spark, "public.sellers")
    suppliers = ler_tabela(spark, "public.suppliers")
    products = ler_tabela(spark, "public.products")
    sales = ler_tabela(spark, "public.sales")
    sales_items = ler_tabela(spark, "public.sales_items")

    dim_date = (
        sales.select(col("date").alias("full_date"))
        .distinct()
        .withColumn("date_key", date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("year", year("full_date"))
        .withColumn("quarter", quarter("full_date"))
        .withColumn("month", month("full_date"))
        .withColumn("day", dayofmonth("full_date"))
        .withColumn("day_of_week", dayofweek("full_date"))
        .withColumn("is_weekend", dayofweek("full_date").isin(1, 7))
        .select("date_key", "full_date", "year", "quarter", "month", "day", "day_of_week", "is_weekend")
    )

    dim_customer = (
        customers
        .withColumn("customer_name", upper("customer_name"))
        .withColumn("email", upper("email"))
        .withColumn("region", regiao(col("state")))
        .select("customer_id", "customer_name", "email", "state", "region")
    )

    dim_seller = (
        sellers
        .withColumn("seller_name", upper("seller_name"))
        .withColumn("email", upper("email"))
        .withColumn("region", regiao(col("state")))
        .select("seller_id", "seller_name", "email", "state", "region", "tx_commission")
    )

    suppliers = suppliers.withColumnRenamed("state", "supplier_state")

    dim_product = (
        products
        .join(categories, "category_id")
        .join(suppliers, "supplier_id")
        .withColumn("product_name", upper("product_name"))
        .withColumn("category_name", upper("category_name"))
        .withColumn("supplier_name", upper("supplier_name"))
        .withColumn("supplier_region", regiao(col("supplier_state")))
        .select(
            "product_id",
            "product_name",
            col("price").alias("list_price"),
            "category_id",
            "category_name",
            "supplier_id",
            "supplier_name",
            "supplier_state",
            "supplier_region",
        )
    )

    fact = (
        sales
        .select("sales_id", "date", "customer_id", "seller_id")
        .join(sales_items, "sales_id")
        .join(sellers.select("seller_id", "tx_commission"), "seller_id")
        .withColumn("date_key", date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("unit_price", round(col("price"), 2))
        .withColumn("line_total", round(col("quantity") * col("price"), 2))
        .withColumn("commission_total", round((col("quantity") * col("price")) * col("tx_commission") / 100, 2))
        .select(
            "sales_id",
            "item_id",
            "date_key",
            "customer_id",
            "seller_id",
            "product_id",
            "quantity",
            "unit_price",
            "line_total",
            "commission_total",
        )
    )

    salvar(dim_date, "dim_date")
    salvar(dim_customer, "dim_customer")
    salvar(dim_seller, "dim_seller")
    salvar(dim_product, "dim_product")
    salvar(fact, "fact_sales_items")

    print("DW carregado com sucesso.")
    print("Linhas na fato:", fact.count())

    spark.stop()


if __name__ == "__main__":
    main()
