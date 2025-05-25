from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
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
    users_silver = SparkSubmitOperator(
        application="/opt/airflow/spark_scripts/bronze_to_silver/users.py",
        task_id="users_silver",
        verbose=True,
        conn_id="spark_default",
        jars="/opt/bitnami/spark/jars/postgresql.jar",
    )


    # entries_silver = SparkSubmitOperator(
    #     task_id="entries_bronze_to_silver",
    #     application="/opt/airflow/pyspark_scripts/bronze_to_silver/entries.py",
    # )

    # activities_silver = SparkSubmitOperator(
    #     task_id="activities_bronze_to_silver",
    #     application="/opt/airflow/pyspark_scripts/bronze_to_silver/activities.py",
    # )

    # # Silver → Gold task
    # denormalized_gold = SparkSubmitOperator(
    #     task_id="create_denormalized",
    #     application="/opt/airflow/pyspark_scripts/silver_to_gold/denormalized.py",
    # )

    # Dependencies
    users_silver
    # [users_silver, entries_silver, activities_silver] >> denormalized_gold
