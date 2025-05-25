"""
Airflow DAG for orchestrating Spark jobs to process and transform data from Bronze to Silver tables.

This DAG defines three main SparkSubmitOperator tasks:
    - silver_users: Transforms user data from Bronze to Silver.
    - silver_entries: Transforms entry data from Bronze to Silver.
    - silver_activities: Transforms activity data from Bronze to Silver.

Each task submits a PySpark script to a Spark cluster using the specified configuration for memory usage and PostgreSQL JDBC connectivity.

DAG Configuration:
    - Name: hisaab_silver_denorm
    - Owner: airflow
    - Schedule: Manual (no schedule interval)
    - Start Date: 2024-01-01
    - Tags: transformation, pyspark
    - Catchup: Disabled
"""
# Each SparkSubmitOperator is configured with:
#   - application: Path to the PySpark script for the transformation.
#   - task_id: Unique identifier for the Airflow task.
#   - verbose: Enables verbose logging for Spark job.
#   - conn_id: Airflow connection ID for Spark.
#   - jars: Path to the PostgreSQL JDBC driver.
#   - conf: Spark configuration dictionary for memory management.

# To enable dependencies or add the Gold denormalization step, uncomment and adjust the relevant sections.

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
}
conf={
    "spark.executor.memory": "1g",  # Reduced from 2g
    "spark.memory.fraction": "0.5",  # Reduced from 0.6
    "spark.executor.memoryOverhead": "256m"  # Reduced from 512m
}
with DAG(
    "hisaab_silver_denorm",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    tags=["transformation","pyspark"],
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

    # # Silver Denorm table task
    # denormalized_gold = SparkSubmitOperator(
    #     task_id="create_denormalized",
    #     application="/opt/airflow/pyspark_scripts/silver_to_gold/denormalized.py",
    # )

    # Dependencies
    silver_entries
    silver_users
    silver_activities
    # [users_silver, entries_silver, activities_silver] >> denormalized_gold
