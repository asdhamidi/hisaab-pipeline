import os
import json
import logging
from uu import encode
from airflow import DAG
from minio import Minio
from bson.json_util import dumps
from pymongo import MongoClient
from io import BytesIO, StringIO
from dotenv import load_dotenv

load_dotenv()

def fetch_mongo_data(database_name: str, collection_name: str) -> str:
    """
    Fetches data from a MongoDB collection and serializes it to a JSON string.

    Args:
        database_name (str): Name of the MongoDB database.
        collection_name (str): Name of the collection to fetch data from.

    Returns:
        str: JSON string of documents (excluding '_id' and 'password' fields).

    Raises:
        Exception: If connection or fetching fails.
    """
    logging.info(f"Fetching data for {database_name}/{collection_name}...")
    MONGODB_URI = os.getenv("MONGODB_URI")

    try:
        # Establish MongoDB connection with timeouts for reliability
        logging.info("Connecting to MongoDB...")
        client = MongoClient(
            MONGODB_URI,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=30000,
            socketTimeoutMS=30000,
        )

        # Verify connection
        client.admin.command("ping")
        logging.info("MongoDB connection established")

        db = client[database_name]
        collection = db[collection_name]

        # Exclude sensitive fields
        results = list(
            collection.find({}, projection={"_id": False, "password": False})
        )

        # Convert MongoDB documents to JSON-string
        data = json.dumps([doc for doc in results])

        if not data:
            logging.warning(f"No data found in {collection_name}")
            return data

        logging.info(
            f"Successfully fetched {len(data)} documents from {collection_name}"
        )
        return data

    except Exception as e:
        raise Exception(
            f"Error in fetch_mongo_data: {str(e)}"
        )
    finally:
        # Ensure connections are closed
        if "client" in locals():
            client.close()

def put_on_bucket(bucket_name: str, object_name: str, data: str) -> bool:
    """
    Uploads data to a MinIO bucket as an object.

    Args:
        bucket_name (str): Name of the MinIO bucket.
        object_name (str): Name of the object to create in the bucket.
        data (str): Data to upload (JSON string).

    Returns:
        bool: True if upload is successful.

    Raises:
        Exception: If upload fails.
    """
    logging.info(f"Starting upload to {bucket_name}/{object_name}...")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")

    try:
        # Connect to MinIO
        logging.info("Connecting to MinIO...")
        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )
        logging.info("MinIO connection established")

        # Ensure bucket exists, create if not
        logging.info(f"Checking if {bucket_name} exists...")
        if not minio_client.bucket_exists(bucket_name):
            logging.info(f"Creating bucket {bucket_name}...")
            minio_client.make_bucket(bucket_name)
        else:
            logging.info(f"{bucket_name} found!")

        # Convert data to BytesIO for upload
        data_stream = BytesIO(data.encode("utf-8"))

        # Upload object to MinIO
        result = minio_client.put_object(
            bucket_name,
            object_name,
            data=data_stream,
            length=len(data),
            content_type="application/json",
        )

        logging.info(f"Successfully uploaded {object_name} (etag: {result.etag})")
        return True

    except Exception as e:
        raise Exception(f"Error in put_on_bucker. Upload failed for {object_name}: {str(e)}")

def mongo_data_ingestion(collection: str) -> dict:
    """
    Orchestrates the ingestion workflow: fetches data from MongoDB and uploads it to MinIO.

    Args:
        collection (str): Name of the MongoDB collection.

    Returns:
        dict: Status dictionary with collection name, status, and document count.

    Raises:
        Exception: If any step fails.
    """
    # Configuration from environment variables
    BUCKET_NAME = os.getenv("MINIO_LANDING_BUCKET")
    MONGODB_DATABASE_NAME = os.getenv("MONGO_DB")

    try:
        data = fetch_mongo_data(MONGODB_DATABASE_NAME, collection)
        if data:
            put_on_bucket(BUCKET_NAME, f"{collection}.json", data)
            return {"collection": collection, "status": "success", "count": len(json.loads(data))}
        else:
            logging.warning(f"Skipping empty collection: {collection}")
            return {"collection": collection, "status": "skipped", "count": 0}

    except Exception as e:
        raise Exception(f"Error in mongo_data_ingestion. Failed processing {collection}: {str(e)}")

def extract_minio_data(bucket_name: str, object_name: str) -> list:
    """
    Downloads and parses JSON data from a MinIO object.

    Args:
        bucket_name (str): Name of the MinIO bucket.
        object_name (str): Name of the object to fetch.

    Returns:
        list: Parsed JSON data.

    Raises:
        Exception: If extraction fails.
    """
    logging.info(f"Extracting data from {bucket_name}/{object_name}...")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")

    try:
        # Connect to MinIO
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

        # Download and decode object data
        data = minio_client.get_object(bucket_name, object_name)
        logging.info(f"Data extracted from {bucket_name}/{object_name} successfully.")
        data = json.loads(data.read().decode("utf-8"))
        return data
    except Exception as e:
        raise Exception(f"Error in extract_minio_data - Extraction failed for {bucket_name}/{object_name}: {str(e)}")

def _generate_insert_query(table_name: str, data: list) -> str:
    """
    Generates a SQL query to truncate and insert data into a Bronze layer table.

    Args:
        table_name (str): Name of the table (without schema).
        data (list): List of dictionaries representing records.

    Returns:
        str: Complete SQL query for truncation and insertion.

    Raises:
        Exception: If data is empty.
    """
    if not data:
        raise Exception("No data to insert into the Bronze layer.")

    TRUNCATE_QUERY = f"TRUNCATE TABLE BRONZE.BRONZE_{table_name.upper()};"
    INSERT_QUERY = f"INSERT INTO BRONZE.BRONZE_{table_name.upper()} VALUES"
    records = []
    for record in data:
        record_data = []
        # Sort keys for consistent column order
        for key in sorted(record.keys()):
            value = record.get(key, "")
            if not value:
                value = ""
            value = str(value)
            value = value.replace("'", "")  # Remove single quotes to avoid SQL errors

            record_data.append(f"'{value}'")

        # Add metadata columns
        record_data.append("current_timestamp")
        record_data.append("current_user")
        record_data = "(" + ", ".join(record_data) + ")"
        records.append(record_data)
    INSERT_QUERY += ", ".join(records)
    INSERT_QUERY += ";"
    COMPLETE_QUERY = TRUNCATE_QUERY + INSERT_QUERY
    return COMPLETE_QUERY

def generate_insert_query(table_name: str) -> str:
    """
    Extracts data from MinIO and generates a SQL insert query for the Bronze layer.

    Args:
        table_name (str): Name of the table (without schema).

    Returns:
        str: Complete SQL query for truncation and insertion.
    """
    BUCKET_NAME = os.getenv("MINIO_LANDING_BUCKET")

    try:
        # Extract data from MinIO
        minio_data = extract_minio_data(BUCKET_NAME, f"{table_name}.json")

        # Generate insert query to insert data into the Bronze layer
        return _generate_insert_query(table_name, minio_data)
    except Exception as e:
        raise Exception(f"Error in generate_insert_query - Failed processing {table_name}: {str(e)}")
