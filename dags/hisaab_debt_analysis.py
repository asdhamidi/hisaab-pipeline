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
    dag_id="hisaab_debt_analysis",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    tags=["gold", "transformation", "pyspark", "debt"],
    catchup=False,
) as dag:
    # gold_debt_ledger = SparkSubmitOperator(
    #     application="/opt/airflow/spark_scripts/silver_to_gold/gold_debt_ledger.py",
    #     task_id="gold_debt_ledger",
    #     verbose=True,
    #     conn_id="spark_default",
    #     jars="/opt/bitnami/spark/jars/postgresql.jar",
    #     conf=conf,
    # )

    gold_user_net_balances = SparkSubmitOperator(
        application="/opt/airflow/spark_scripts/silver_to_gold/gold_user_net_balances.py",
        task_id="gold_user_net_balances",
        verbose=True,
        conn_id="spark_default",
        jars="/opt/bitnami/spark/jars/postgresql.jar",
        conf=conf,
    )

    # Dependencies
    # gold_debt_ledger
    gold_user_net_balances
