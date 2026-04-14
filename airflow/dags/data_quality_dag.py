"""
Airflow DAG: Data Quality Checks.
Runs Great Expectations suites on all pipeline layers after each run.
"""
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {"owner":"data-engineering","depends_on_past":False,
    "email":[os.getenv("ALERT_EMAIL","alerts@example.com")],"email_on_failure":True,
    "retries":1,"retry_delay":timedelta(minutes=3),"sla":timedelta(hours=1)}

dag = DAG(dag_id="data_quality_checks", default_args=default_args,
    description="Great Expectations validation on all layers",
    schedule_interval=None, start_date=datetime(2024,1,1), catchup=False,
    tags=["retail","quality"])

def run_validation(suite, checkpoint, **ctx):
    import great_expectations as gx
    ge_ctx = gx.get_context(context_root_dir=os.getenv("GE_ROOT_DIR","/opt/great_expectations"))
    result = ge_ctx.run_checkpoint(checkpoint_name=checkpoint)
    if not result.success:
        raise ValueError(f"{suite} validation failed")
    return f"{suite} passed"

def run_anomaly_detection(**ctx):
    import sys; sys.path.insert(0, "/opt/great_expectations/plugins")
    from anomaly_detection import AnomalyDetector
    anomalies = AnomalyDetector().detect_transaction_count_anomalies(lookback_days=30, std_threshold=2.0)
    if anomalies:
        raise ValueError(f"Anomalies detected: {len(anomalies)} days flagged")

v_bronze = PythonOperator(task_id="validate_bronze", python_callable=run_validation,
    op_kwargs={"suite":"Bronze","checkpoint":"bronze_checkpoint"}, dag=dag)
v_silver = PythonOperator(task_id="validate_silver", python_callable=run_validation,
    op_kwargs={"suite":"Silver","checkpoint":"silver_checkpoint"}, dag=dag)
v_gold = PythonOperator(task_id="validate_gold", python_callable=run_validation,
    op_kwargs={"suite":"Gold","checkpoint":"gold_checkpoint"}, dag=dag)
anomaly = PythonOperator(task_id="detect_anomalies", python_callable=run_anomaly_detection, dag=dag)

notify_pass = EmailOperator(task_id="notify_pass", to=os.getenv("ALERT_EMAIL","alerts@example.com"),
    subject="[OK] Quality Checks Passed", html_content="All validations passed: {{ ds }}",
    trigger_rule=TriggerRule.ALL_SUCCESS, dag=dag)
notify_fail = EmailOperator(task_id="notify_fail", to=os.getenv("ALERT_EMAIL","alerts@example.com"),
    subject="[CRITICAL] Quality Check Failed", html_content="Quality check failed: {{ ds }}",
    trigger_rule=TriggerRule.ONE_FAILED, dag=dag)

v_bronze >> v_silver >> v_gold >> anomaly >> [notify_pass, notify_fail]
