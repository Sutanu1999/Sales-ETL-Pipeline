"""
Data Quality Checks
Runs validation checks on bronze, silver, and gold layers.
Raises an exception if any critical check fails, so Airflow can mark the
run as failed and alert accordingly.

Checks performed:
- Row count sanity (tables are not empty / haven't dropped below expected minimum)
- Null checks on primary key / critical columns
- Primary key uniqueness
- Referential integrity between gold fact and dimension tables
- Value range checks (e.g. price/payment values must be non-negative)
"""

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "utils"))
from spark_session import get_spark_session

SILVER_DIR = "data/silver"
GOLD_DIR = "data/gold"

# Track failures so we can report everything at the end instead of stopping
# at the first failed check.
failures = []


def check(condition, message):
    if condition:
        print(f"  [PASS] {message}")
    else:
        print(f"  [FAIL] {message}")
        failures.append(message)


def read_delta(spark, base_dir, table_name):
    return spark.read.format("delta").load(os.path.join(base_dir, table_name))


def check_not_empty(df, table_name, min_rows=1):
    count = df.count()
    check(count >= min_rows, f"{table_name}: row count >= {min_rows} (actual: {count})")


def check_no_nulls(df, table_name, columns):
    for col_name in columns:
        null_count = df.filter(df[col_name].isNull()).count()
        check(null_count == 0, f"{table_name}.{col_name}: no nulls (found: {null_count})")


def check_unique_key(df, table_name, key_columns):
    total = df.count()
    distinct = df.select(*key_columns).distinct().count()
    check(
        total == distinct,
        f"{table_name}: primary key {key_columns} is unique (total={total}, distinct={distinct})",
    )


def check_non_negative(df, table_name, columns):
    for col_name in columns:
        negative_count = df.filter(df[col_name] < 0).count()
        check(negative_count == 0, f"{table_name}.{col_name}: no negative values (found: {negative_count})")


def check_value_range(df, table_name, column, min_value, max_value):
    """
    Checks that all non-null values in a numeric column fall within [min_value, max_value].
    Catches type/content corruption (e.g. text accidentally landing in a numeric column)
    that structural checks like null/uniqueness/non-negative would miss.
    """
    out_of_range_count = df.filter(
        df[column].isNotNull() & ((df[column] < min_value) | (df[column] > max_value))
    ).count()
    check(
        out_of_range_count == 0,
        f"{table_name}.{column}: all values within [{min_value}, {max_value}] (found {out_of_range_count} out of range)",
    )


def check_referential_integrity(fact_df, fact_key, dim_df, dim_key, fact_name, dim_name):
    orphan_count = (
        fact_df.select(fact_key)
        .distinct()
        .join(dim_df.select(dim_key), fact_df[fact_key] == dim_df[dim_key], "left_anti")
        .count()
    )
    check(
        orphan_count == 0,
        f"{fact_name}.{fact_key} -> {dim_name}.{dim_key}: no orphan records (found: {orphan_count})",
    )


def run_silver_checks(spark):
    print("\n--- Silver Layer Checks ---")

    customers = read_delta(spark, SILVER_DIR, "customers")
    check_not_empty(customers, "silver.customers")
    check_no_nulls(customers, "silver.customers", ["customer_id"])
    check_unique_key(customers, "silver.customers", ["customer_id"])

    orders = read_delta(spark, SILVER_DIR, "orders")
    check_not_empty(orders, "silver.orders")
    check_no_nulls(orders, "silver.orders", ["order_id", "customer_id"])
    check_unique_key(orders, "silver.orders", ["order_id"])

    order_items = read_delta(spark, SILVER_DIR, "order_items")
    check_not_empty(order_items, "silver.order_items")
    check_non_negative(order_items, "silver.order_items", ["price", "freight_value"])

    order_payments = read_delta(spark, SILVER_DIR, "order_payments")
    check_non_negative(order_payments, "silver.order_payments", ["payment_value"])

    order_reviews = read_delta(spark, SILVER_DIR, "order_reviews")
    check_not_empty(order_reviews, "silver.order_reviews")
    check_no_nulls(order_reviews, "silver.order_reviews", ["review_id", "order_id"])
    check_unique_key(order_reviews, "silver.order_reviews", ["review_id"])
    check_value_range(order_reviews, "silver.order_reviews", "review_score", 1, 5)

    products = read_delta(spark, SILVER_DIR, "products")
    check_unique_key(products, "silver.products", ["product_id"])

    sellers = read_delta(spark, SILVER_DIR, "sellers")
    check_unique_key(sellers, "silver.sellers", ["seller_id"])


def run_gold_checks(spark):
    print("\n--- Gold Layer Checks ---")

    dim_customer = read_delta(spark, GOLD_DIR, "dim_customer")
    dim_product = read_delta(spark, GOLD_DIR, "dim_product")
    dim_seller = read_delta(spark, GOLD_DIR, "dim_seller")
    fact_order_items = read_delta(spark, GOLD_DIR, "fact_order_items")
    fact_payments = read_delta(spark, GOLD_DIR, "fact_payments")
    fact_reviews = read_delta(spark, GOLD_DIR, "fact_reviews")

    check_not_empty(fact_order_items, "gold.fact_order_items")
    check_not_empty(fact_payments, "gold.fact_payments")
    check_not_empty(fact_reviews, "gold.fact_reviews")

    check_unique_key(dim_customer, "gold.dim_customer", ["customer_id"])
    check_unique_key(dim_product, "gold.dim_product", ["product_id"])
    check_unique_key(dim_seller, "gold.dim_seller", ["seller_id"])

    check_non_negative(fact_order_items, "gold.fact_order_items", ["price", "freight_value"])
    check_non_negative(fact_payments, "gold.fact_payments", ["payment_value"])
    check_value_range(fact_reviews, "gold.fact_reviews", "review_score", 1, 5)
    check_unique_key(fact_reviews, "gold.fact_reviews", ["review_id"])

    # Referential integrity: every fact_order_items row should map to a real customer/seller/product
    check_referential_integrity(
        fact_order_items, "customer_id", dim_customer, "customer_id",
        "gold.fact_order_items", "gold.dim_customer",
    )
    check_referential_integrity(
        fact_order_items, "seller_id", dim_seller, "seller_id",
        "gold.fact_order_items", "gold.dim_seller",
    )
    check_referential_integrity(
        fact_order_items, "product_id", dim_product, "product_id",
        "gold.fact_order_items", "gold.dim_product",
    )


if __name__ == "__main__":
    spark = get_spark_session("DataQualityChecks")
    try:
        run_silver_checks(spark)
        run_gold_checks(spark)

        print("\n--- Summary ---")
        if failures:
            print(f"{len(failures)} check(s) FAILED:")
            for f in failures:
                print(f"  - {f}")
            raise SystemExit(1)
        else:
            print("All data quality checks passed.")
    finally:
        spark.stop()