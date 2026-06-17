# Local Sales Data Pipeline

End-to-end batch ETL pipeline using PySpark, Delta Lake, Airflow, and MySQL — built entirely with free, local tools.

## Architecture

```
Raw CSVs --> Bronze (Delta) --> Silver (cleaned) --> Gold (star schema) --> MySQL warehouse
                              orchestrated by Airflow (Docker)
```

- **Bronze**: Raw data ingested as-is into Delta tables, with minimal transformation (schema inference + ingestion metadata)
- **Silver**: Cleaned, deduplicated, type-corrected data
- **Gold**: Star schema (fact and dimension tables) ready for analytics
- **Warehouse**: Gold tables loaded into MySQL for SQL querying
- **Orchestration**: Apache Airflow (Docker), running a custom image with PySpark/Delta/Java, schedules and chains all 4 pipeline stages

## Tech Stack

- PySpark (local mode)
- Delta Lake
- Apache Airflow (Docker, custom image)
- MySQL
- Dataset: [Olist Brazilian E-commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

## Project Structure

```
Sales-ETL-Pipeline/
├── data/
│   ├── raw/        # Source CSVs (gitignored)
│   ├── bronze/      # Raw Delta tables (gitignored)
│   ├── silver/      # Cleaned Delta tables (gitignored)
│   └── gold/        # Star schema Delta tables (gitignored)
├── jars/
│   └── mysql-connector-j-9.7.0.jar   # MySQL JDBC driver (gitignored)
├── sql/
│   └── analysis_queries.sql      # Analytical queries on the warehouse
├── src/
│   ├── utils/
│   │   └── spark_session.py    # Reusable Spark session with Delta + JDBC support
│   ├── 01_ingest_bronze.py      # Bronze layer ingestion
│   ├── 02_transform_silver.py   # Silver layer cleaning
│   ├── 03_build_gold.py         # Gold layer star schema
│   └── 04_load_warehouse.py     # Load gold tables into MySQL
├── airflow/
│   ├── dags/
│   │   └── sales_etl_dag.py      # Orchestrates all 4 pipeline stages
│   ├── logs/                      # Airflow task logs (gitignored, runtime-generated)
│   ├── plugins/                   # Airflow plugins (gitignored, empty/unused currently)
│   ├── config/                    # Airflow config overrides (gitignored, empty/unused currently)
│   ├── Dockerfile                 # Custom Airflow image with Java + PySpark + Delta
│   └── docker-compose.yaml        # Airflow services (webserver, scheduler, worker, etc.)
├── .env                           # MySQL credentials for venv runs (gitignored)
├── .env.docker                    # MySQL credentials for Airflow/Docker runs (gitignored)
├── requirements.txt
├── README.md
└── INSIGHTS.md                    # Key findings from the warehouse
```

## Star Schema (Gold Layer)

- **dim_customer** — customer_id, location
- **dim_product** — product_id, category (joined with English translation), dimensions
- **dim_seller** — seller_id, location
- **dim_date** — generated calendar dimension spanning the order date range
- **fact_order_items** — grain: one row per order item (price, freight, order status/date)
- **fact_payments** — grain: one row per payment installment
- **fact_reviews** — grain: one row per review

## Setup

1. Create a virtual environment (Python 3.11 recommended) and activate it
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Download the [Olist dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) and extract the CSVs into `data/raw/`
4. Download [MySQL Connector/J](https://dev.mysql.com/downloads/connector/j/) and place the jar in `jars/`
5. Create a MySQL database (`CREATE DATABASE sales_dw;`) and add a `.env` file with:
   ```
   MYSQL_USER=root
   MYSQL_PASSWORD=your_password
   MYSQL_HOST=localhost
   MYSQL_PORT=3306
   MYSQL_DATABASE=sales_dw
   ```
6. For Airflow, also create `.env.docker` with the same values but `MYSQL_HOST=host.docker.internal`

## Running the Pipeline

### Bronze layer (raw ingestion)
```bash
python src/01_ingest_bronze.py
```
Reads all 9 raw CSVs and writes them as Delta tables in `data/bronze/`.

### Silver layer (cleaning)
```bash
python src/02_transform_silver.py
```
Cleans, deduplicates, and standardizes each bronze table (null-key filtering, type casting, text normalization). Notably removes ~3.4K duplicate reviews and ~280K duplicate geolocation entries present in the raw data.

### Gold layer (star schema)
```bash
python src/03_build_gold.py
```
Builds 4 dimension tables and 3 fact tables (order items, payments, reviews) from the silver layer, ready for analytical querying.

### Load to MySQL warehouse
```bash
python src/04_load_warehouse.py
```
Loads all 7 gold tables into the `sales_dw` MySQL database via JDBC.

### Run analysis queries
```bash
mysql -u root -p sales_dw < sql/analysis_queries.sql
```
10 queries covering revenue trends, top categories/sellers, payment mix, review scores, and order status. See [INSIGHTS.md](INSIGHTS.md) for key findings.

### Airflow orchestration
```bash
cd airflow
docker compose up -d
```
Open `http://localhost:8080` (default login: airflow/airflow), enable the `sales_etl_pipeline` DAG, and trigger it. All 4 stages (bronze → silver → gold → warehouse load) run in sequence inside Docker, using a custom Airflow image with Java, PySpark, and Delta Lake preinstalled.

## Status

- [x] Project setup
- [x] Bronze layer ingestion
- [x] Silver layer cleaning
- [x] Gold layer star schema
- [x] MySQL warehouse load
- [x] Analytical SQL queries
- [x] Airflow DAG (Docker, custom image with PySpark/Delta/Java)
- [ ] Data quality checks
- [ ] Dashboard (optional)