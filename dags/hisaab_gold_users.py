from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
}

conf = {
    "spark.executor.memory": "1g",  # Reduced from 2g
    "spark.memory.fraction": "0.5",  # Reduced from 0.6
    "spark.executor.memoryOverhead": "256m",  # Reduced from 512m
}

with DAG(
    "hisaab_gold_users",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    tags=["gold", "transformation", "pyspark"],
    catchup=False,
) as dag:
    spending_per_user_monthly = SparkSubmitOperator(
        application="/opt/airflow/spark_scripts/silver_to_gold/spending_per_user_monthly.py",
        task_id="spending_per_user_monthly",
        verbose=True,
        conn_id="spark_default",
        jars="/opt/bitnami/spark/jars/postgresql.jar",
        conf=conf,
    )

    # Dependencies
    spending_per_user_monthly
