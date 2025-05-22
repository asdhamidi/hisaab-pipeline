import json
import logging
from minio import Minio
from airflow import DAG
from ast import literal_eval
from datetime import datetime
from airflow.models import Variable
from airflow.decorators import task
from utils.ingestion import mongo_data_ingestion
from airflow.providers.postgres.operators.postgres import PostgresOperator
from json import loads

default_args = {
    "owner": "airflow",
}


def extract_minio_data(bucket_name, object_name):
    logging.info(f"Extracting data from {bucket_name}/{object_name}...")
    MINIO_ACCESS_KEY = Variable.get("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = Variable.get("MINIO_SECRET_KEY")
    MINIO_ENDPOINT = Variable.get("MINIO_ENDPOINT", "minio:9000")

    try:
        # MinIO connection
        logging.info("Connecting to MinIO...")
        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )
        logging.info("MinIO connection established")

        # Check if the bucket exists
        if not minio_client.bucket_exists(bucket_name):
            raise Exception(f"Bucket {bucket_name} does not exist.")
        logging.info(f"Bucket {bucket_name} exists.")

        # Check if the object exists
        if not minio_client.stat_object(bucket_name, object_name):
            raise Exception(
                f"Object {object_name} does not exist in bucket {bucket_name}."
            )
        logging.info(f"Object {object_name} exists in bucket {bucket_name}.")

        # Get the object data
        data = minio_client.get_object(bucket_name, object_name)
        logging.info(f"Data extracted from {bucket_name}/{object_name} successfully.")
        data = json.loads(data.read().decode("utf-8"))
        return data
    except Exception as e:
        raise Exception(f"extract_minio_data failed: {str(e)}")


def _generate_insert_query(table_name, data):
    """
    Inserts data into the Bronze layer.
    """
    if not data:
        raise Exception("No data to insert into the Bronze layer.")

    TRUNCATE_QUERY = f"TRUNCATE TABLE BRONZE.BRONZE_{table_name.upper()};"
    INSERT_QUERY = f"INSERT INTO BRONZE.BRONZE_{table_name.upper()} VALUES"
    records = []
    for record in data:
        record_data = []
        for key in sorted(record.keys()):
            value = record.get(key, "")
            if not value: value = ""
            value = str(value)
            value = value.replace("'", "")

            record_data.append(f"'{value}'")
        record_data.append("current_timestamp")
        record_data.append("current_user")
        record_data = "(" + ", ".join(record_data) + ")"
        records.append(record_data)
    INSERT_QUERY += ", ".join(records)
    INSERT_QUERY += ";"
    COMPLETE_QUERY = TRUNCATE_QUERY + INSERT_QUERY
    return COMPLETE_QUERY

def generate_insert_query(table_name):
    """
    Ingests data from MinIO to the Bronze layer.
    """
    BUCKET_NAME = Variable.get("MINIO_LANDING_BUCKET")

    # Extract data from MinIO
    minio_data = extract_minio_data(BUCKET_NAME, f"{table_name}.json")

    # Generate insert query to insert data into the Bronze layer
    return _generate_insert_query(table_name, minio_data)



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
        sql=generate_insert_query("users")
    )

    insert_data_into_entries = PostgresOperator(
        task_id="insert_data_into_entries",
        postgres_conn_id="hisaab_postgres",
        sql=generate_insert_query("entries")
    )

    insert_data_into_activities = PostgresOperator(
        task_id="insert_data_into_activities",
        postgres_conn_id="hisaab_postgres",
        sql=generate_insert_query("activities")
    )

[insert_data_into_users, insert_data_into_entries, insert_data_into_activities]
