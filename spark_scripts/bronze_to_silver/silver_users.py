import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp


def transform_users():
    """
    Extracts user data from the 'bronze.bronze_users' table in a PostgreSQL database,
    applies necessary transformations, and writes the processed data to the
    'silver.silver_users' table.

    Workflow:
        1. Establishes a SparkSession.
        2. Reads user data from the 'bronze.bronze_users' table using JDBC.
        3. Transforms the 'user_created_at' column to a timestamp using the format 'd/M/yy h:mm a'.
        4. Writes the transformed DataFrame to the 'silver.silver_users' table in overwrite mode.
        5. Handles exceptions and ensures the SparkSession is stopped.

    Raises:
        Exception: Propagates any exception that occurs during the ETL process.

    Notes:
        - Assumes the existence of the 'bronze.bronze_users' and 'silver.silver_users' tables.
        - Requires the PostgreSQL JDBC driver and appropriate user credentials.
        - Designed for use in a local Spark environment.
    """
    spark = None
    try:
        spark = SparkSession.builder.master("local").getOrCreate()

        # Read from bronze.users
        df_bronze_users = (
            spark.read.format("jdbc")
            .option("url", "jdbc:postgresql://postgres:5432/hisaab_analytics")
            .option("dbtable", "bronze.bronze_users")
            .option("user", "airflow")
            .option("password", "airflow")
            .option("driver", "org.postgresql.Driver")
            .load()
        )

        # Transformations
        df_silver_users = df_bronze_users.withColumn(
            "user_created_at", to_timestamp(col("user_created_at"), "d/M/yy h:mm a")
        )

        # Write to silver.users
        properties = {
            "user": "airflow",
            "password": "airflow",
            "driver": "org.postgresql.Driver",
        }
        df_silver_users.write.jdbc(
            url="jdbc:postgresql://postgres:5432/hisaab_analytics",
            table="silver.silver_users",
            mode="overwrite",
            properties=properties,
        )

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    logging.info("Starting transformation for BRONZE.BRONZE_USERS")
    transform_users()
    logging.info("Transformation completed for BRONZE.BRONZE_USERS")
