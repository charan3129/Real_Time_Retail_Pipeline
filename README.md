# Real-Time Retail Data Pipeline

End-to-end retail analytics pipeline: Kafka streaming ingestion, PySpark processing through Bronze/Silver/Gold medallion architecture with Delta Lake, Snowflake warehouse, Airflow orchestration, Great Expectations validation, and Streamlit dashboard.

## Architecture
```
Kafka Producer -> Spark Streaming -> Bronze (Delta) -> Silver (Delta) -> Gold (Delta)
                                                                           |
                                                                     Snowflake
                                                                           |
                                                                     Streamlit Dashboard
Airflow orchestrates all stages. Great Expectations validates each layer.
```

## Tech Stack
| Component | Technology |
|-----------|-----------|
| Ingestion | Apache Kafka |
| Stream Processing | PySpark Structured Streaming |
| Batch Processing | PySpark |
| Storage | Delta Lake |
| Warehouse | Snowflake |
| Orchestration | Apache Airflow |
| Transformation | dbt |
| Data Quality | Great Expectations |
| Dashboard | Streamlit |
| Deployment | Docker Compose |
| CI/CD | GitHub Actions |

## Quick Start
```bash
git clone <your-repo>
cd Real_Time_Retail_Pipeline
cp config/pipeline_config.yml config/pipeline_config.yml.bak  # backup
# Edit .env with your Snowflake creds
docker-compose up -d
```

| Service | URL |
|---------|-----|
| Airflow | http://localhost:8080 |
| Streamlit | http://localhost:8501 |
| Spark UI | http://localhost:8081 |

See [docs/SETUP_GUIDE.md](docs/SETUP_GUIDE.md) for detailed instructions.
See [docs/DATA_DICTIONARY.md](docs/DATA_DICTIONARY.md) for table documentation.
