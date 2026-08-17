"""
port_congestion_dag.py
------------------------
DAG يومي يشغّل مهمة Spark التي تحوّل بيانات Silver إلى تقرير ازدحام
الموانئ في طبقة Gold. يعمل داخل حاوية Spark عبر spark-submit.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "mohamed",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="port_congestion_daily_report",
    description="تحليل يومي لازدحام الموانئ الإماراتية بناءً على بيانات AIS",
    default_args=default_args,
    schedule_interval="0 3 * * *",  # كل يوم الساعة 3 فجرًا
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["maritime", "spark", "gold-layer"],
) as dag:

    run_gold_job = BashOperator(
        task_id="run_port_congestion_spark_job",
        bash_command=(
            "docker exec maritime-spark-master /opt/spark/bin/spark-submit "
            "/opt/spark-apps/silver_to_gold_port_congestion.py"
        ),
    )

    run_gold_job
