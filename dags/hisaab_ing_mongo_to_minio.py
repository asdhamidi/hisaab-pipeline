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
