"""
Silver Layer Transformation
Reads bronze Delta tables, applies cleaning/standardization, writes to data/silver.

Cleaning rules applied per table:
- Drop exact duplicate rows
- Drop rows with null primary keys
- Trim/standardize string columns
- Cast date columns to proper timestamp type
- Drop bronze metadata columns (_ingested_at, _source_file) - re-added fresh in silver
"""

import os
import sys
from pyspark.sql.functions import col, trim, lower, current_timestamp, to_timestamp

sys.path.append(os.path.join(os.path.dirname(__file__), "utils"))
from spark_session import get_spark_session

BRONZE_DIR = "data/bronze"
SILVER_DIR = "data/silver"


def read_bronze(spark, table_name):
    path = os.path.join(BRONZE_DIR, table_name)
    df = spark.read.format("delta").load(path)
    # Drop bronze metadata columns before cleaning
    return df.drop("_ingested_at", "_source_file")


def write_silver(df, table_name):
    df = df.withColumn("_processed_at", current_timestamp())
    path = os.path.join(SILVER_DIR, table_name)
    df.write.format("delta").mode("overwrite").save(path)
    print(f"  -> Silver written: {table_name} ({df.count()} rows)\n")


def clean_customers(spark):
    print("Cleaning customers ...")
    df = read_bronze(spark, "customers")
    df = df.dropDuplicates(["customer_id"])
    df = df.filter(col("customer_id").isNotNull())
    df = df.withColumn("customer_city", trim(lower(col("customer_city"))))
    df = df.withColumn("customer_state", trim(col("customer_state")))
    write_silver(df, "customers")


def clean_orders(spark):
    print("Cleaning orders ...")
    df = read_bronze(spark, "orders")
    df = df.dropDuplicates(["order_id"])
    df = df.filter(col("order_id").isNotNull() & col("customer_id").isNotNull())

    timestamp_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    for c in timestamp_cols:
        df = df.withColumn(c, to_timestamp(col(c)))

    df = df.withColumn("order_status", trim(lower(col("order_status"))))
    write_silver(df, "orders")


def clean_order_items(spark):
    print("Cleaning order_items ...")
    df = read_bronze(spark, "order_items")
    df = df.dropDuplicates(["order_id", "order_item_id"])
    df = df.filter(
        col("order_id").isNotNull()
        & col("product_id").isNotNull()
        & (col("price") >= 0)
    )
    df = df.withColumn("shipping_limit_date", to_timestamp(col("shipping_limit_date")))
    write_silver(df, "order_items")


def clean_order_payments(spark):
    print("Cleaning order_payments ...")
    df = read_bronze(spark, "order_payments")
    df = df.dropDuplicates(["order_id", "payment_sequential"])
    df = df.filter(col("order_id").isNotNull() & (col("payment_value") >= 0))
    df = df.withColumn("payment_type", trim(lower(col("payment_type"))))
    write_silver(df, "order_payments")


def clean_order_reviews(spark):
    print("Cleaning order_reviews ...")
    df = read_bronze(spark, "order_reviews")
    df = df.dropDuplicates(["review_id"])
    df = df.filter(col("review_id").isNotNull() & col("order_id").isNotNull())
    df = df.withColumn("review_creation_date", to_timestamp(col("review_creation_date")))
    df = df.withColumn("review_answer_timestamp", to_timestamp(col("review_answer_timestamp")))
    write_silver(df, "order_reviews")


def clean_products(spark):
    print("Cleaning products ...")
    df = read_bronze(spark, "products")
    df = df.dropDuplicates(["product_id"])
    df = df.filter(col("product_id").isNotNull())
    df = df.withColumn("product_category_name", trim(lower(col("product_category_name"))))
    write_silver(df, "products")


def clean_sellers(spark):
    print("Cleaning sellers ...")
    df = read_bronze(spark, "sellers")
    df = df.dropDuplicates(["seller_id"])
    df = df.filter(col("seller_id").isNotNull())
    df = df.withColumn("seller_city", trim(lower(col("seller_city"))))
    df = df.withColumn("seller_state", trim(col("seller_state")))
    write_silver(df, "sellers")


def clean_geolocation(spark):
    print("Cleaning geolocation ...")
    df = read_bronze(spark, "geolocation")
    # Geolocation has many true duplicates (same zip/lat/lng reported repeatedly)
    df = df.dropDuplicates(
        ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"]
    )
    df = df.filter(
        col("geolocation_zip_code_prefix").isNotNull()
        & col("geolocation_lat").isNotNull()
        & col("geolocation_lng").isNotNull()
    )
    write_silver(df, "geolocation")


def clean_category_translation(spark):
    print("Cleaning category_translation ...")
    df = read_bronze(spark, "category_translation")
    df = df.dropDuplicates(["product_category_name"])
    df = df.filter(col("product_category_name").isNotNull())
    write_silver(df, "category_translation")


if __name__ == "__main__":
    spark = get_spark_session("SilverTransformation")
    try:
        clean_customers(spark)
        clean_orders(spark)
        clean_order_items(spark)
        clean_order_payments(spark)
        clean_order_reviews(spark)
        clean_products(spark)
        clean_sellers(spark)
        clean_geolocation(spark)
        clean_category_translation(spark)
        print("Silver transformation completed successfully.")
    finally:
        spark.stop()