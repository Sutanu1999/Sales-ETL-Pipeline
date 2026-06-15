# Local Sales Data Pipeline

End-to-end batch ETL pipeline using PySpark, Delta Lake, Airflow, and MySQL — built entirely with free, local tools.

## Architecture

```
Raw CSVs --> Bronze (Delta) --> Silver (cleaned) --> Gold (star schema) --> MySQL warehouse
```

- **Bronze**: Raw data ingested as-is into Delta tables, with minimal transformation (schema inference + ingestion metadata)
- **Silver**: Cleaned, deduplicated, type-corrected data
- **Gold**: Star schema (fact and dimension tables) ready for analytics
- **Warehouse**: Gold tables loaded into MySQL for SQL querying
- **Orchestration**: Apache Airflow (Docker) to schedule the pipeline

## Tech Stack

- PySpark (local mode)
- Delta Lake
- Apache Airflow (Docker)
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
├── src/
│   ├── utils/
│   │   └── spark_session.py   # Reusable Spark session with Delta support
│   └── 01_ingest_bronze.py     # Bronze layer ingestion
├── requirements.txt
└── README.md
```

## Setup

1. Create a virtual environment (Python 3.11 recommended) and activate it
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Download the [Olist dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) and extract the CSVs into `data/raw/`

## Running the Pipeline

### Bronze layer (raw ingestion)
```bash
python src/01_ingest_bronze.py
```
Reads all 9 raw CSVs and writes them as Delta tables in `data/bronze/`.

### Silver layer (cleaning)
🚧 In progress

### Gold layer (star schema)
🚧 Planned

### Load to MySQL warehouse
🚧 Planned

### Airflow orchestration
🚧 Planned

## Status

- [x] Project setup
- [x] Bronze layer ingestion
- [ ] Silver layer cleaning
- [ ] Gold layer star schema
- [ ] MySQL warehouse load
- [ ] Airflow DAG
- [ ] Data quality checks
- [ ] Dashboard (optional)