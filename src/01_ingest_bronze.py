"""
Bronze Layer Ingestion
Reads raw CSV files from data/raw and writes them as Delta tables in data/bronze.
Minimal transformation: just schema inference + metadata columns.
"""

import os
import sys
from pyspark.sql.functions import current_timestamp, input_file_name

# Allow importing from src/utils
sys.path.append(os.path.join(os.path.dirname(__file__), "utils"))
from spark_session import get_spark_session

RAW_DIR = "data/raw"
BRONZE_DIR = "data/bronze"

# Map of source CSV files -> target bronze table names
SOURCE_FILES = {
    "olist_customers_dataset.csv": "customers",
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "olist_geolocation_dataset.csv": "geolocation",
    "product_category_name_translation.csv": "category_translation",
}


def ingest_to_bronze(spark):
    for csv_file, table_name in SOURCE_FILES.items():
        raw_path = os.path.join(RAW_DIR, csv_file)
        bronze_path = os.path.join(BRONZE_DIR, table_name)

        print(f"Reading {csv_file} ...")
        df = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(raw_path)
        )

        # Add metadata columns
        df = df.withColumn("_ingested_at", current_timestamp())
        df = df.withColumn("_source_file", input_file_name())

        row_count = df.count()
        print(f"  -> {row_count} rows")

        print(f"Writing to bronze: {bronze_path}")
        df.write.format("delta").mode("overwrite").save(bronze_path)
        print(f"  -> Done: {table_name}\n")


if __name__ == "__main__":
    spark = get_spark_session("BronzeIngestion")
    try:
        ingest_to_bronze(spark)
        print("Bronze ingestion completed successfully.")
    finally:
        spark.stop()