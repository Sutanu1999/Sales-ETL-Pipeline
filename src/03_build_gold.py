"""
Gold Layer - Star Schema
Reads silver Delta tables, builds dimension and fact tables, writes to data/gold.

Tables produced:
- dim_customer
- dim_product
- dim_seller
- dim_date
- fact_order_items   (grain: one row per order item)
- fact_payments      (grain: one row per payment installment)
- fact_reviews       (grain: one row per review)
"""

import os
import sys
from pyspark.sql.functions import (
    col,
    current_timestamp,
    explode,
    sequence,
    to_date,
    year,
    month,
    quarter,
    dayofweek,
    date_format,
    lit,
    min as spark_min,
    max as spark_max,
)

sys.path.append(os.path.join(os.path.dirname(__file__), "utils"))
from spark_session import get_spark_session

SILVER_DIR = "data/silver"
GOLD_DIR = "data/gold"


def read_silver(spark, table_name):
    path = os.path.join(SILVER_DIR, table_name)
    return spark.read.format("delta").load(path)


def write_gold(df, table_name):
    df = df.withColumn("_loaded_at", current_timestamp())
    path = os.path.join(GOLD_DIR, table_name)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    print(f"  -> Gold written: {table_name} ({df.count()} rows)\n")


def build_dim_customer(spark):
    print("Building dim_customer ...")
    df = read_silver(spark, "customers")
    dim = df.select(
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    ).dropDuplicates(["customer_id"])
    write_gold(dim, "dim_customer")
    return dim


def build_dim_product(spark):
    print("Building dim_product ...")
    products = read_silver(spark, "products")
    translation = read_silver(spark, "category_translation")

    dim = products.join(
        translation, on="product_category_name", how="left"
    ).select(
        "product_id",
        "product_category_name",
        col("product_category_name_english").alias("category_name_english"),
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ).dropDuplicates(["product_id"])
    write_gold(dim, "dim_product")
    return dim


def build_dim_seller(spark):
    print("Building dim_seller ...")
    df = read_silver(spark, "sellers")
    dim = df.select(
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
    ).dropDuplicates(["seller_id"])
    write_gold(dim, "dim_seller")
    return dim


def build_dim_date(spark, orders_df):
    print("Building dim_date ...")
    date_range = orders_df.select(
        spark_min(to_date(col("order_purchase_timestamp"))).alias("min_date"),
        spark_max(to_date(col("order_purchase_timestamp"))).alias("max_date"),
    ).collect()[0]

    min_date, max_date = date_range["min_date"], date_range["max_date"]

    dim = spark.createDataFrame([(1,)], ["dummy"]).select(
        explode(sequence(lit(min_date), lit(max_date))).alias("date")
    )

    dim = (
        dim.withColumn("date_id", date_format(col("date"), "yyyyMMdd").cast("int"))
        .withColumn("year", year(col("date")))
        .withColumn("month", month(col("date")))
        .withColumn("quarter", quarter(col("date")))
        .withColumn("day_of_week", dayofweek(col("date")))
        .withColumn("month_name", date_format(col("date"), "MMMM"))
        .withColumn("day_name", date_format(col("date"), "EEEE"))
    )
    write_gold(dim, "dim_date")
    return dim


def build_fact_order_items(spark):
    print("Building fact_order_items ...")
    order_items = read_silver(spark, "order_items")
    orders = read_silver(spark, "orders").select(
        "order_id", "customer_id", "order_status", "order_purchase_timestamp"
    )

    fact = order_items.join(orders, on="order_id", how="left").select(
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "customer_id",
        "order_status",
        to_date(col("order_purchase_timestamp")).alias("order_date"),
        date_format(col("order_purchase_timestamp"), "yyyyMMdd").cast("int").alias("order_date_id"),
        "price",
        "freight_value",
        "shipping_limit_date",
    )
    write_gold(fact, "fact_order_items")


def build_fact_payments(spark):
    print("Building fact_payments ...")
    df = read_silver(spark, "order_payments")
    fact = df.select(
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
    )
    write_gold(fact, "fact_payments")


def build_fact_reviews(spark):
    print("Building fact_reviews ...")
    df = read_silver(spark, "order_reviews")
    fact = df.select(
        "review_id",
        "order_id",
        "review_score",
        to_date(col("review_creation_date")).alias("review_date"),
        date_format(col("review_creation_date"), "yyyyMMdd").cast("int").alias("review_date_id"),
        "review_answer_timestamp",
    )
    write_gold(fact, "fact_reviews")


if __name__ == "__main__":
    spark = get_spark_session("GoldStarSchema")
    try:
        build_dim_customer(spark)
        build_dim_product(spark)
        build_dim_seller(spark)

        orders_df = read_silver(spark, "orders")
        build_dim_date(spark, orders_df)

        build_fact_order_items(spark)
        build_fact_payments(spark)
        build_fact_reviews(spark)

        print("Gold layer star schema build completed successfully.")
    finally:
        spark.stop()