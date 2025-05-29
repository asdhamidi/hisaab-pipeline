"""
Airflow DAG for orchestrating the transformation and denormalization of data from bronze to silver layer using Spark.

This DAG performs the following operations:
1. Executes three Spark jobs to process and transform raw (bronze) data into cleaned (silver) tables:
    - Users data (`silver_users`)
    - Entries data (`silver_entries`)
    - Activities data (`silver_activities`)
2. After successful completion of all three silver table jobs, runs a denormalization Spark job (`silver_hisaab_denorm`)
   that joins the processed tables into a single denormalized table.

Key Features:
- Uses SparkSubmitOperator to submit PySpark scripts to a Spark cluster.
- Configures Spark jobs with reduced memory settings for optimized resource usage.
- Ensures that the denormalization step only runs after all upstream silver table jobs have completed.
- Designed for manual or external triggering (no schedule interval).
- Tags the DAG for easy discovery and categorization in Airflow UI.

Dependencies:
- All Spark jobs require access to the PostgreSQL JDBC driver for data connectivity.
- Assumes Spark and Airflow are properly configured with the necessary connections and permissions.

"""

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
    "hisaab_silver_denorm",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    tags=["transformation", "pyspark"],
    catchup=False,
) as dag:

    # Bronze → Silver tasks
    silver_users = SparkSubmitOperator(
        application="/opt/airflow/spark_scripts/bronze_to_silver/silver_users.py",
        task_id="silver_users",
        verbose=True,
        conn_id="spark_default",
        jars="/opt/bitnami/spark/jars/postgresql.jar",
        conf=conf,
    )

    silver_entries = SparkSubmitOperator(
        application="/opt/airflow/spark_scripts/bronze_to_silver/silver_entries.py",
        task_id="silver_entries",
        verbose=True,
        conn_id="spark_default",
        jars="/opt/bitnami/spark/jars/postgresql.jar",
        conf=conf,
    )

    silver_activities = SparkSubmitOperator(
        application="/opt/airflow/spark_scripts/bronze_to_silver/silver_activities.py",
        task_id="silver_activities",
        verbose=True,
        conn_id="spark_default",
        jars="/opt/bitnami/spark/jars/postgresql.jar",
        conf=conf,
    )

    # Denormalized table made by joining above tables
    silver_hisaab_denorm = SparkSubmitOperator(
        application="/opt/airflow/spark_scripts/bronze_to_silver/silver_hisaab_denorm.py",
        task_id="silver_hisaab_denorm",
        verbose=True,
        conn_id="spark_default",
        jars="/opt/bitnami/spark/jars/postgresql.jar",
        conf=conf,
    )

    # Dependencies
    [silver_users, silver_entries, silver_activities] >> silver_hisaab_denorm
