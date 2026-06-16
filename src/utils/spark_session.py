import os
import sys
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def get_spark_session(app_name: str = "SalesETL") -> SparkSession:
    """
    Returns a SparkSession configured for local Windows execution with
    Delta Lake support and the MySQL JDBC driver on the classpath.
    """
    python_path = sys.executable

    os.environ["PYSPARK_PYTHON"] = python_path
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

    # Path to the MySQL JDBC driver jar (relative to project root)
    jdbc_jar_path = os.path.abspath("jars/mysql-connector-j-9.7.0.jar")

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.pyspark.python", python_path)
        .config("spark.pyspark.driver.python", python_path)
        .config("spark.jars", jdbc_jar_path)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", "spark-warehouse")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark