"""
DAG for Ingesting Data from MinIO to Bronze Layer in PostgreSQL

This Airflow DAG orchestrates the ingestion of data from MinIO storage into the bronze layer of a PostgreSQL database. 
It leverages the `PostgresOperator` to execute dynamically generated SQL insert queries for multiple tables.

Tasks:
    - insert_data_into_users: Inserts user data into the 'users' table.
    - insert_data_into_entries: Inserts entry data into the 'entries' table.
    - insert_data_into_activities: Inserts activity data into the 'activities' table.

Note:
    Ensure that the Airflow connection 'hisaab_postgres' is properly configured and that the 
    `generate_insert_query` utility is implemented and accessible in the project.
"""
import json
import logging
from json import loads
from minio import Minio
from airflow import DAG
from ast import literal_eval
from datetime import datetime
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator

from utils.ingestion import generate_insert_query

default_args = {
    "owner": "airflow",
}

with DAG(
    "hisaab_minio_to_bronze",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    tags=["minio", "bronze", "ingestion"],
    catchup=False,
) as dag:

    insert_data_into_users = PostgresOperator(
        task_id="insert_data_into_users",
        postgres_conn_id="hisaab_postgres",
        sql=generate_insert_query("users"),
    )

    insert_data_into_entries = PostgresOperator(
        task_id="insert_data_into_entries",
        postgres_conn_id="hisaab_postgres",
        sql=generate_insert_query("entries"),
    )

    insert_data_into_activities = PostgresOperator(
        task_id="insert_data_into_activities",
        postgres_conn_id="hisaab_postgres",
        sql=generate_insert_query("activities"),
    )

[insert_data_into_users, insert_data_into_entries, insert_data_into_activities]
