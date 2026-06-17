"""
Sales ETL Pipeline DAG
Orchestrates the medallion architecture pipeline:
Bronze -> Silver -> Gold -> MySQL Warehouse

Each task runs the corresponding script from the mounted project directory.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/project"

default_args = {
    "owner": "sutanu",
    "retries": 1,
}

with DAG(
    dag_id="sales_etl_pipeline",
    description="Bronze -> Silver -> Gold -> MySQL warehouse load for sales data",
    default_args=default_args,
    schedule_interval=None,  # manual trigger only; change to a cron expression for scheduling
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["sales", "etl", "pyspark", "delta", "mysql"],
) as dag:

    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command=f"cd {PROJECT_DIR} && python src/01_ingest_bronze.py",
    )

    transform_silver = BashOperator(
        task_id="transform_silver",
        bash_command=f"cd {PROJECT_DIR} && python src/02_transform_silver.py",
    )

    build_gold = BashOperator(
        task_id="build_gold",
        bash_command=f"cd {PROJECT_DIR} && python src/03_build_gold.py",
    )

    load_warehouse = BashOperator(
        task_id="load_warehouse",
        bash_command=f"cd {PROJECT_DIR} && RUNNING_IN_DOCKER=true python src/04_load_warehouse.py",
    )

    ingest_bronze >> transform_silver >> build_gold >> load_warehouse