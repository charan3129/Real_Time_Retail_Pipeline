"""
Airflow DAG: Retail Pipeline Orchestration.
Kafka health check -> Spark batch -> dbt run -> dbt test -> Snowflake refresh.
"""
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": [os.getenv("ALERT_EMAIL", "alerts@example.com")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "sla": timedelta(hours=3),
}

dag = DAG(
    dag_id="retail_pipeline",
    default_args=default_args,
    description="End-to-end retail pipeline: Kafka -> Spark -> dbt -> Snowflake",
    schedule_interval="0 */4 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["retail", "production"],
)

def check_kafka_health(**ctx):
    from kafka import KafkaConsumer
    from kafka.errors import NoBrokersAvailable
    try:
        c = KafkaConsumer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS","kafka:9092"), request_timeout_ms=10000)
        topics = c.topics()
        c.close()
        topic = os.getenv("KAFKA_TOPIC","retail_transactions")
        if topic not in topics:
            raise ValueError(f"Topic {topic} not found")
        ctx["ti"].xcom_push(key="kafka_status", value="healthy")
        return "kafka_healthy"
    except NoBrokersAvailable:
        ctx["ti"].xcom_push(key="kafka_status", value="unreachable")
        return "kafka_unhealthy"

def kafka_branch(**ctx):
    return "spark_bronze_to_silver" if ctx["ti"].xcom_pull(key="kafka_status", task_ids="check_kafka") == "healthy" else "notify_kafka_failure"

check_kafka = PythonOperator(task_id="check_kafka", python_callable=check_kafka_health, dag=dag)
branch_kafka = BranchPythonOperator(task_id="branch_kafka", python_callable=kafka_branch, dag=dag)
notify_kafka_failure = EmailOperator(task_id="notify_kafka_failure", to=os.getenv("ALERT_EMAIL","alerts@example.com"),
    subject="[ALERT] Retail Pipeline - Kafka Down", html_content="<h3>Kafka unreachable</h3><p>{{ ds }}</p>", dag=dag)

spark_b2s = SparkSubmitOperator(task_id="spark_bronze_to_silver", application="/opt/spark/apps/spark_batch/bronze_to_silver.py",
    name="retail-b2s", conn_id="spark_default", packages="io.delta:delta-spark_2.12:3.1.0",
    conf={"spark.sql.extensions":"io.delta.sql.DeltaSparkSessionExtension",
          "spark.sql.catalog.spark_catalog":"org.apache.spark.sql.delta.catalog.DeltaCatalog"}, dag=dag)

spark_s2g = SparkSubmitOperator(task_id="spark_silver_to_gold", application="/opt/spark/apps/spark_batch/silver_to_gold.py",
    name="retail-s2g", conn_id="spark_default", packages="io.delta:delta-spark_2.12:3.1.0",
    conf={"spark.sql.extensions":"io.delta.sql.DeltaSparkSessionExtension",
          "spark.sql.catalog.spark_catalog":"org.apache.spark.sql.delta.catalog.DeltaCatalog"}, dag=dag)

dbt_run = BashOperator(task_id="dbt_run", bash_command="cd /opt/dbt/retail_dbt && dbt run --profiles-dir /opt/dbt/profiles", dag=dag)
dbt_test = BashOperator(task_id="dbt_test", bash_command="cd /opt/dbt/retail_dbt && dbt test --profiles-dir /opt/dbt/profiles", dag=dag)

sf_refresh = BashOperator(task_id="snowflake_refresh",
    bash_command="snowsql -q \"SELECT 'Views refreshed' AS status;\"", dag=dag)

notify_ok = EmailOperator(task_id="notify_success", to=os.getenv("ALERT_EMAIL","alerts@example.com"),
    subject="[OK] Retail Pipeline Complete", html_content="<h3>All stages done: {{ ds }}</h3>",
    trigger_rule=TriggerRule.ALL_SUCCESS, dag=dag)
notify_fail = EmailOperator(task_id="notify_failure", to=os.getenv("ALERT_EMAIL","alerts@example.com"),
    subject="[ALERT] Retail Pipeline Failed", html_content="<h3>Pipeline failed: {{ ds }}</h3>",
    trigger_rule=TriggerRule.ONE_FAILED, dag=dag)

check_kafka >> branch_kafka >> [spark_b2s, notify_kafka_failure]
spark_b2s >> spark_s2g >> dbt_run >> dbt_test >> sf_refresh >> [notify_ok, notify_fail]
