"""
DAG for ingesting data from MongoDB collections to MinIO using Airflow.

This DAG retrieves a list of MongoDB collections from an Airflow Variable, then processes each collection by invoking
a custom ingestion function. The ingestion logic is encapsulated in the `mongo_data_ingestion` utility.

Tasks:
    - get_collections: Fetches the list of MongoDB collections to ingest from the Airflow Variable 'MONGO_COLLECTION_LIST'.
    - process_collection: Ingests data for a single MongoDB collection into MinIO.

Airflow Variables:
    MONGO_COLLECTION_LIST (str): JSON-encoded list of MongoDB collection names to be ingested.

Raises:
    Exception: If the 'MONGO_COLLECTION_LIST' Airflow Variable is not set or is empty.

Dependencies:
    - utils.ingestion.mongo_data_ingestion: Custom ingestion function for MongoDB to MinIO.
"""
from airflow import DAG
from ast import literal_eval
from datetime import datetime
from airflow.models import Variable
from airflow.decorators import task
from utils.ingestion import mongo_data_ingestion
from json import loads

default_args = {
    "owner": "airflow",
}

extract_collections_data_exec = lambda: literal_eval(Variable.get("MONGO_COLLECTION_LIST"))

with DAG(
    "hisaab_ing_mongo_to_minio",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    tags=["mongo", "minio", "ingestion"],
    catchup=False,
) as dag:

    @task
    def get_collections():
        collection_list = Variable.get("MONGO_COLLECTION_LIST", None)
        if not collection_list:
            raise Exception("No MongoDB collection list found.")
        return loads(collection_list)

    @task
    def process_collection(collection):
        return mongo_data_ingestion(collection)

    collections = get_collections()
    process_collection.expand(collection=collections)
