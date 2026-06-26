# Spark ETL Pipeline on Azure Databricks

[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://www.databricks.com/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)](https://www.snowflake.com/)
[![BigQuery](https://img.shields.io/badge/BigQuery-4285F4?style=for-the-badge&logo=googlebigquery&logoColor=white)](https://cloud.google.com/bigquery)

> High-performance PySpark batch ETL pipeline on Azure Databricks processing **200M+ records daily** for telecom analytics — featuring partition pruning, broadcast joins, incremental SCD loads, and Apache Airflow orchestration.

---

## Overview

This project implements a production-scale **PySpark batch ETL pipeline** built on Azure Databricks that processes large-scale telecom usage and customer datasets. The pipeline ingests raw data, applies complex transformations, implements historical tracking via SCD patterns, and loads curated data into Snowflake and BigQuery for downstream analytics and BI consumption.

Key outcomes delivered:
- Processes **200M+ records daily** across telecom usage and customer datasets
- Reduced nightly batch job runtime **from 3+ hours to under 1 hour** through Spark optimizations
- Implemented incremental SCD logic enabling reliable historical trending analysis
- Eliminated recurring pipeline failures through robust retry and alerting patterns

---

## Architecture

```
Raw Data Sources (Azure ADLS Gen2 / Blob Storage)
        |
        v
Azure Databricks (PySpark ETL)
   - Bronze Layer: Raw ingestion with schema enforcement
   - Silver Layer: Cleaned, deduplicated, enriched data
   - Gold Layer: Aggregated, business-ready datasets
        |
        v
Target Systems
   - Snowflake (reporting and analytics warehouse)
   - BigQuery (ML feature store and ad-hoc analytics)
        |
        v
Apache Airflow (Orchestration & Monitoring)
        |
        v
Tableau / BI Dashboards
```

---

## Project Structure

```
spark-etl-pipeline-databricks/
├── src/
│   ├── ingestion/
│   │   ├── bronze_loader.py          -- raw ingestion with schema validation
│   │   ├── schema_registry.py        -- centralized schema definitions
│   │   └── source_connectors.py      -- ADLS Gen2, Blob, Kafka connectors
│   ├── transformations/
│   │   ├── usage_transformer.py      -- telecom usage event processing
│   │   ├── customer_transformer.py   -- customer profile enrichment
│   │   ├── network_transformer.py    -- network performance aggregations
│   │   └── scd_handler.py            -- SCD Type 1 & 2 logic
│   ├── loaders/
│   │   ├── snowflake_loader.py       -- bulk load to Snowflake
│   │   └── bigquery_loader.py        -- streaming/batch load to BigQuery
│   └── utils/
│       ├── spark_session.py          -- Spark session configuration
│       ├── data_quality.py           -- DQ checks and profiling
│       └── metrics_logger.py         -- pipeline metrics tracking
├── dags/
│   ├── telecom_daily_etl_dag.py      -- daily batch DAG
│   ├── customer_scd_dag.py           -- SCD refresh DAG
│   └── network_perf_dag.py           -- near real-time NiFi DAG
├── notebooks/
│   ├── exploratory/
│   │   ├── usage_profiling.ipynb
│   │   └── customer_segmentation.ipynb
│   └── production/
│       ├── daily_usage_etl.ipynb
│       └── customer_scd_refresh.ipynb
├── configs/
│   ├── spark_config.yaml             -- cluster and Spark configs
│   ├── pipeline_config.yaml          -- source/target configs
│   └── quality_rules.yaml            -- data quality rule definitions
├── tests/
│   ├── test_transformations.py
│   ├── test_scd_handler.py
│   └── test_data_quality.py
└── requirements.txt
```

---

## Key Features

### Spark Performance Optimizations
- **Partition Pruning**: Partition datasets by `event_date` and `region_code` to minimize data scan
- **Broadcast Joins**: Broadcast small dimension tables (<200MB) to eliminate shuffle overhead on fact joins
- **Predicate Pushdown**: Push filters to source layer to reduce data read volume
- **Caching**: Cache frequently reused intermediate DataFrames to avoid recomputation
- **Repartitioning**: Dynamic repartition before wide transformations to balance executor load

```python
# Example: Broadcast join optimization
from pyspark.sql.functions import broadcast

usage_enriched = (
    usage_df
    .join(broadcast(customer_dim_df), "customer_id", "left")
    .join(broadcast(network_dim_df), "cell_tower_id", "left")
    .filter(col("event_date") >= current_date() - 1)
)
```

### Incremental Load Strategy
- Delta-based incremental loads using high-watermark timestamps
- Avoids full table scans on 200M+ record datasets
- Configurable lookback window for late-arriving data

```python
# Incremental load with watermark
def get_incremental_data(spark, table, last_run_ts):
    return spark.read.format("delta").load(table).filter(
        col("updated_at") > last_run_ts
    )
```

### SCD Type 1 & 2 Implementation
- **SCD Type 1**: Overwrite current values for non-historical attributes
- **SCD Type 2**: Track full history with `effective_from`, `effective_to`, `is_current` flags
- Merge-based upsert logic using Delta Lake `MERGE INTO` for atomic updates

### Data Quality Framework
- Schema validation at ingestion (Bronze layer)
- Null checks, range validations, and referential integrity at Silver layer
- Record count reconciliation between source and target
- Automated alerting via Airflow on quality threshold breach

### Apache Airflow Orchestration
- Daily DAG with task-level dependencies and retries
- SLA alerting — PagerDuty notification if job exceeds 90-minute SLA
- Apache NiFi for near real-time streaming data flows
- Backfill support for reprocessing historical partitions

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Compute | Azure Databricks (PySpark) |
| Storage | Azure ADLS Gen2, Delta Lake |
| Targets | Snowflake, BigQuery |
| Orchestration | Apache Airflow, Apache NiFi |
| Language | Python, PySpark, SQL, SparkSQL |
| Monitoring | Databricks Jobs UI, Airflow UI |
| Version Control | Git, GitHub |

---

## Performance Results

| Metric | Before | After |
|--------|--------|-------|
| Nightly batch runtime | 3+ hours | < 1 hour |
| Daily record volume | 200M+ records | 200M+ records |
| Shuffle data per job | ~800 GB | ~180 GB |
| Pipeline failure rate | ~15% | < 2% |
| Data freshness SLA | Missed regularly | 99%+ adherence |

---

## Getting Started

```bash
# Clone the repository
git clone https://github.com/NikhilRacha83/spark-etl-pipeline-databricks.git
cd spark-etl-pipeline-databricks

# Install dependencies
pip install -r requirements.txt

# Configure pipeline settings
cp configs/pipeline_config.yaml.example configs/pipeline_config.yaml
# Edit with your ADLS, Snowflake, and BigQuery credentials

# Run tests
pytest tests/ -v

# Submit Spark job (local mode)
spark-submit src/ingestion/bronze_loader.py --env local --date 2026-06-01

# Deploy Airflow DAGs
cp dags/*.py $AIRFLOW_HOME/dags/
```

---

## Data Domains

| Domain | Volume | Key Transformations |
|--------|--------|-------------------|
| Usage Events | ~120M records/day | Sessionization, aggregation by tower/region |
| Customer Data | ~80M records/day | SCD Type 2, segmentation, churn scoring |
| Network Performance | ~10M records/day | Percentile calculations, anomaly flagging |

---

## Author

**Nikhil Rachagani** | Data Engineer  
[LinkedIn](https://www.linkedin.com/in/nikhil-r1102/) | [GitHub](https://github.com/NikhilRacha83)# spark-etl-pipeline-databricks
PySpark batch ETL pipeline on Azure Databricks processing 200M+ records daily - partition optimization, broadcast joins, incremental loads, SCD logic, and Airflow orchestration for telecom analytics
