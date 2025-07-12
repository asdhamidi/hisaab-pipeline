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
    "hisaab_expense_patterns",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    tags=["gold", "transformation", "pyspark", "trends"],
    catchup=False,
) as dag:
    gold_expense_stats = SparkSubmitOperator(
        application="/opt/airflow/spark_scripts/silver_to_gold/gold_expense_stats.py",
        task_id="gold_expense_stats",
        verbose=True,
        conn_id="spark_default",
        jars="/opt/bitnami/spark/jars/postgresql.jar",
        conf=conf,
    )

    gold_expense_categories = SparkSubmitOperator(
        application="/opt/airflow/spark_scripts/silver_to_gold/gold_expense_categories.py",
        task_id="gold_expense_categories",
        verbose=True,
        conn_id="spark_default",
        jars="/opt/bitnami/spark/jars/postgresql.jar",
        conf=conf,
    )

    # WIP
    gold_top_items = SparkSubmitOperator(
        application="/opt/airflow/spark_scripts/silver_to_gold/gold_top_items.py",
        task_id="gold_top_items",
        verbose=True,
        conn_id="spark_default",
        jars="/opt/bitnami/spark/jars/postgresql.jar",
        conf=conf,
    )

# Dependencies
gold_expense_stats >> gold_expense_categories >> gold_top_items
