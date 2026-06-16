"""
Load Gold Layer to MySQL Warehouse
Reads gold Delta tables and writes them to MySQL via JDBC.
"""

import os
import sys
from dotenv import load_dotenv

sys.path.append(os.path.join(os.path.dirname(__file__), "utils"))
from spark_session import get_spark_session

load_dotenv()

GOLD_DIR = "data/gold"

MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

JDBC_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"

TABLES = [
    "dim_customer",
    "dim_product",
    "dim_seller",
    "dim_date",
    "fact_order_items",
    "fact_payments",
    "fact_reviews",
]


def load_table_to_mysql(spark, table_name):
    print(f"Loading {table_name} into MySQL ...")
    path = os.path.join(GOLD_DIR, table_name)
    df = spark.read.format("delta").load(path)

    df.write \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", table_name) \
        .option("user", MYSQL_USER) \
        .option("password", MYSQL_PASSWORD) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("overwrite") \
        .save()

    print(f"  -> Loaded {table_name} ({df.count()} rows)\n")


if __name__ == "__main__":
    if not all([MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE]):
        raise ValueError(
            "Missing MySQL credentials. Check that .env contains "
            "MYSQL_USER, MYSQL_PASSWORD, and MYSQL_DATABASE."
        )

    spark = get_spark_session("LoadWarehouse")
    try:
        for table in TABLES:
            load_table_to_mysql(spark, table)
        print("All gold tables loaded into MySQL successfully.")
    finally:
        spark.stop()