# Real-Time Retail Data Pipeline

End-to-end retail analytics pipeline that ingests streaming transaction data via Apache Kafka, processes it through a Bronze/Silver/Gold medallion architecture using PySpark and Delta Lake, loads aggregated data into Snowflake, orchestrates workflows with Airflow, validates data quality with Great Expectations, and visualizes insights through an interactive Streamlit dashboard.

## Architecture

📐 **[View Full Architecture Diagram (PDF)](docs/architecture_diagram.pdf)** — Visual system architecture with all components, data flow, and layer details

📊 **[View Pipeline Flowchart (PDF)](docs/pipeline_flowchart.pdf)** — 10-step data flow showing inputs, transformations, and outputs at each stage

```
  ┌──────────────┐      ┌──────────────┐      ┌────────────────────────────────────────────┐
  │    Kafka     │      │    Spark     │      │            DELTA LAKE STORAGE              │
  │   Producer   │─────▶│  Structured  │─────▶│                                            │
  │   (Faker)    │      │  Streaming   │      │  ┌────────┐   ┌────────┐   ┌────────────┐ │
  └──────────────┘      └──────────────┘      │  │ BRONZE │──▶│ SILVER │──▶│    GOLD    │ │
                                               │  │  Raw   │   │ Clean  │   │ Star Schema│ │
                                               │  └────────┘   └────────┘   └─────┬──────┘ │
                                               └──────────────────────────────────┬────────┘
                                                                                   │
  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐          ┌─────────▼────────┐
  │   Airflow    │─────▶│     dbt      │─────▶│    Great     │          │    Snowflake     │
  │  Scheduling  │      │   Models &   │      │ Expectations │          │ Views + Snowpipe │
  │  SLA Alerts  │      │    Tests     │      │  Validation  │          └─────────┬────────┘
  └──────────────┘      └──────────────┘      └──────────────┘                    │
                                                                           ┌──────▼───────┐
  ┌──────────────┐      ┌──────────────┐                                   │   Streamlit  │
  │    Docker    │      │   GitHub     │                                   │  Dashboard   │
  │   Compose    │      │   Actions    │                                   │  Analytics   │
  └──────────────┘      └──────────────┘                                   └──────────────┘
```

## How It Works — Data Flow

| Step | Component | What Happens | Output |
|------|-----------|-------------|--------|
| 1 | Kafka Producer | Generates synthetic retail events using Faker (1-5s interval) with edge cases: ~3% nulls, ~2% duplicates, ~5% late arrivals | JSON events on `retail_transactions` topic |
| 2 | Spark Streaming | Reads from Kafka, parses JSON, adds metadata (ingestion_ts, batch_id, row_hash), validates order_id | Bronze Delta Lake table (raw + metadata) |
| 3 | Spark Batch (B→S) | Deduplicates by order_id, fills nulls with defaults, standardizes types, flags late arrivals | Silver Delta Lake table (~98% of Bronze, unique, clean) |
| 4 | Spark Batch (S→G) | Builds Kimball star schema: fact_sales + dim_product (SCD2) + dim_store + dim_date | Gold Delta Lake tables with SHA-256 surrogate keys |
| 5 | Snowflake | Loads Gold data via Snowpipe/COPY INTO, creates 6 analytical views with clustering keys | Queryable warehouse with optimized views |
| 6 | dbt | SQL transformations: staging views, intermediate joins, mart tables with automated tests | Tested, documented models with referential integrity |
| 7 | Great Expectations | Validates each layer: schema checks, uniqueness, value ranges, Z-score anomaly detection | Validation reports, data docs, anomaly alerts |
| 8 | Airflow | Orchestrates full pipeline every 4 hours: health check → Spark → dbt → Snowflake → alerts | SLA monitoring, email notifications |
| 9 | Streamlit | Interactive dashboard: KPIs, revenue trends, top products, store performance, 24h volume | Live charts with date/region/category filters |

## Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Ingestion | Apache Kafka | Real-time event streaming with partitioning |
| Stream Processing | PySpark Structured Streaming | Continuous Bronze layer writes |
| Batch Processing | PySpark + Delta Lake | Silver/Gold transformations with ACID |
| Storage | Delta Lake | Medallion architecture with time travel |
| Data Warehouse | Snowflake | Analytical queries with micro-partitioning |
| Orchestration | Apache Airflow | Scheduling, SLA monitoring, alerting |
| Transformation | dbt | SQL models, automated testing, documentation |
| Data Quality | Great Expectations | Validation suites, anomaly detection |
| Dashboard | Streamlit + Plotly | Interactive analytics with caching |
| Deployment | Docker Compose | Full-stack local deployment |
| CI/CD | GitHub Actions | Lint, test, validate on every push |

## Project Structure

```
├── kafka_producer/          # Event generator (Faker + edge cases)
├── spark_streaming/         # Structured Streaming → Bronze Delta Lake
├── spark_batch/             # Bronze → Silver → Gold transformations
├── snowflake/               # DDL, views, Snowpipe, analytical queries
├── airflow/dags/            # Pipeline orchestration + quality DAGs
├── dbt_project/             # SQL models, macros, seeds, tests
├── great_expectations/      # Validation suites + anomaly detection
├── streamlit/               # Interactive analytics dashboard
├── docker/                  # Dockerfiles (Spark, Airflow, Streamlit)
├── tests/                   # pytest suite (18 tests)
├── config/                  # Pipeline config + logging
├── data/seed/               # Sample data for offline testing
├── docs/
│   ├── architecture_diagram.pdf  # Visual architecture diagram
│   ├── pipeline_flowchart.pdf    # Step-by-step data flow
│   ├── SETUP_GUIDE.md            # Detailed setup instructions
│   └── DATA_DICTIONARY.md        # All tables and columns
├── .github/workflows/ci.yml # CI/CD pipeline
├── docker-compose.yml        # Full stack definition
└── .gitignore
```

## Quick Start

### Prerequisites
- Docker Desktop (v24+) and Docker Compose v2+
- Python 3.9+
- Snowflake account (free trial at https://signup.snowflake.com)

### 1. Clone and Configure
```bash
git clone <your-repo-url>
cd Real_Time_Retail_Pipeline

# Create .env file with your Snowflake credentials
cat > .env << EOF
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=RETAIL_WH
EOF
```

### 2. Start All Services
```bash
docker-compose up -d
```

### 3. Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | admin / admin |
| Streamlit Dashboard | http://localhost:8501 | — |
| Spark Master UI | http://localhost:8081 | — |

### 4. Trigger the Pipeline
```bash
docker exec -it retail-airflow airflow dags trigger retail_pipeline
```

### 5. Run Tests Locally
```bash
pip install pytest numpy faker
pytest tests/ -v
```

## Documentation

- [Setup Guide](docs/SETUP_GUIDE.md) — Step-by-step environment setup and troubleshooting
- [Data Dictionary](docs/DATA_DICTIONARY.md) — Complete table and column documentation
- [Architecture Diagram](docs/architecture_diagram.pdf) — Visual system architecture
- [Pipeline Flowchart](docs/pipeline_flowchart.pdf) — Data flow through all 10 stages

## License

MIT License — free for educational and portfolio use.
