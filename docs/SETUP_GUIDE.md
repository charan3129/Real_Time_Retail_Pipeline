# Setup Guide

## Prerequisites
- Docker Desktop (v24+) and Docker Compose v2+
- Python 3.9+
- Snowflake account (free trial at https://signup.snowflake.com)
- 8 GB RAM for Docker

## Step 1: Clone
```bash
git clone <your-repo>
cd Real_Time_Retail_Pipeline
```

## Step 2: Configure Environment
Create `.env` in project root (gitignored):
```bash
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=RETAIL_WH
SNOWFLAKE_DATABASE=RETAIL_ANALYTICS
SNOWFLAKE_ROLE=SYSADMIN
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=retail_transactions
ALERT_EMAIL=alerts@example.com
```

## Step 3: Snowflake Setup
Run DDL scripts in order in Snowflake Worksheets:
1. `snowflake/ddl/01_database_setup.sql`
2. `snowflake/ddl/02_raw_tables.sql`
3. `snowflake/ddl/03_gold_tables.sql`
4. `snowflake/ddl/04_views.sql`
5. `snowflake/ddl/05_snowpipe.sql` (uncomment for your cloud storage)

## Step 4: Start Docker Stack
```bash
docker-compose up -d --build
docker-compose ps  # verify all containers running
```

## Step 5: Verify Kafka
```bash
docker logs retail-kafka-producer --tail 20
```

## Step 6: Access Services
| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| Streamlit | http://localhost:8501 | none |
| Spark UI | http://localhost:8081 | none |

## Step 7: Configure dbt
```bash
cp dbt_project/profiles.yml.template ~/.dbt/profiles.yml
# Edit with your Snowflake creds
cd dbt_project && dbt debug && dbt run && dbt test
```

## Step 8: Trigger Pipeline
```bash
docker exec -it retail-airflow airflow dags trigger retail_pipeline
docker exec -it retail-airflow airflow dags trigger data_quality_checks
```

## Step 9: Run Tests
```bash
pip install pytest numpy faker kafka-python
pytest tests/ -v
```

## Stopping
```bash
docker-compose down     # stop
docker-compose down -v  # stop + remove data
```
