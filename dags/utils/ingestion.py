import os
import json
import logging
from io import BytesIO, StringIO
from minio import Minio
from airflow import DAG
from bson.json_util import dumps
from pymongo import MongoClient
from airflow.models import Variable
from uu import encode

def fetch_mongo_data(database_name: str, collection_name: str) -> str:
    """Fetch data from MongoDB with proper BSON serialization"""
    logging.info(f"Fetching data for {database_name}/{collection_name}...")
    MONGODB_URI = Variable.get("MONGODB_URI")

    try:
        # MongoDB connection with timeout
        logging.info("Connecting to MongoDB...")
        client = MongoClient(
            MONGODB_URI,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=30000,
            socketTimeoutMS=30000
        )

        # Verify connection
        client.admin.command('ping')
        logging.info("MongoDB connection established")

        db = client[database_name]
        collection = db[collection_name]
        results = list(collection.find({}, projection={"_id": False, "password": False}))

        # Convert MongoDB documents to JSON-string
        data = json.dumps([doc for doc in results])

        if not data:
            logging.warning(f"No data found in {collection_name}")
            return []

        logging.info(f"Successfully fetched {len(data)} documents from {collection_name}")
        return data

    except Exception as e:
        raise Exception(
            f"Error in fetching from {collection_name} does not exist. :{str(e)}")
    finally:
        # Ensure connections are closed
        if 'client' in locals():
            client.close()

def put_on_bucket(bucket_name: str, object_name: str, data: list) -> bool:
    """Upload data to MinIO with enhanced error handling"""
    logging.info(f"Starting upload to {bucket_name}/{object_name}...")
    MINIO_ACCESS_KEY = Variable.get('MINIO_ACCESS_KEY')
    MINIO_SECRET_KEY = Variable.get('MINIO_SECRET_KEY')
    MINIO_ENDPOINT =  Variable.get('MINIO_ENDPOINT', 'minio:9000')

    try:
        # MinIO connection
        logging.info("Connecting to MinIO...")
        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
        logging.info("MinIO connection established")

        # Ensuring landing bucket exists
        logging.info(f"Checking if {bucket_name} exists...")
        if not minio_client.bucket_exists(bucket_name):
            logging.info(f"Creating bucket {bucket_name}...")
            minio_client.make_bucket(bucket_name)
        else:
            logging.info(f"{bucket_name} found!")

        # Convert data to BytesIO Object
        data_stream = BytesIO(data.encode("utf-8"))

        # Upload with metadata
        result = minio_client.put_object(
            bucket_name,
            object_name,
            data=data_stream,
            length=len(data),
            content_type='application/json'
        )

        logging.info(f"Successfully uploaded {object_name} (etag: {result.etag})")
        return True

    except Exception as e:
        raise Exception(f"Upload failed for {object_name}: {str(e)}")



def mongo_data_ingestion(collection):
    """Main ingestion workflow with resource management"""
    # Configuration
    BUCKET_NAME = Variable.get("MINIO_LANDING_BUCKET")
    MONGODB_DATABASE_NAME = Variable.get('MONGO_DB')

    try:
        data = fetch_mongo_data(MONGODB_DATABASE_NAME, collection)
        if data:
            put_on_bucket(BUCKET_NAME, f"{collection}.json", data)
            return {"collection": collection, "status": "success", "count": len(data)}
        else:
            logging.warning(f"Skipping empty collection: {collection}")
            return {"collection": collection, "status": "skipped", "count": 0}

    except Exception as e:
        raise Exception(f"Failed processing {collection}: {str(e)}")
