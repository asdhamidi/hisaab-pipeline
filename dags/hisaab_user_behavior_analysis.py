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
    dag_id="hisaab_user_behavior_analysis",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    tags=["gold", "transformation", "pyspark", "behavior"],
    catchup=False,
) as dag:
    gold_busiest_days = SparkSubmitOperator(
        application="/opt/airflow/spark_scripts/silver_to_gold/gold_busiest_days.py",
        task_id="gold_busiest_days",
        verbose=True,
        conn_id="spark_default",
        jars="/opt/bitnami/spark/jars/postgresql.jar",
        conf=conf,
    )

    gold_user_engagement = SparkSubmitOperator(
        application="/opt/airflow/spark_scripts/silver_to_gold/gold_user_engagement.py",
        task_id="gold_user_engagement",
        verbose=True,
        conn_id="spark_default",
        jars="/opt/bitnami/spark/jars/postgresql.jar",
        conf=conf,
    )

    gold_user_activities = SparkSubmitOperator(
        application="/opt/airflow/spark_scripts/silver_to_gold/gold_user_activities.py",
        task_id="gold_user_activities",
        verbose=True,
        conn_id="spark_default",
        jars="/opt/bitnami/spark/jars/postgresql.jar",
        conf=conf,
    )

# Dependencies
gold_busiest_days >> gold_user_engagement >> gold_user_activities
