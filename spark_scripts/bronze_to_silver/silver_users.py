import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys
from pathlib import Path

# Add the parent directory to Python's module search path
sys.path.append(str(Path(__file__).parent.parent))

from utils.utils import get_table_for_spark, write_data_to_table

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
        spark = SparkSession.builder.master("local").appName("silver_users") .getOrCreate()

        # Read from bronze.users
        df_bronze_users = get_table_for_spark(spark, "bronze.bronze_users")

        # Transformations
        df_silver_users = df_bronze_users.withColumn(
            "user_created_at", F.to_timestamp(F.col("user_created_at"), "d/M/yy h:mm a")
        ).withColumn(
            "admin",
            F.when(
                F.col("admin").eqNullSafe(""),
                F.lit(False)
            ).otherwise(F.col("admin").cast("boolean"))
        )

        write_data_to_table(df_silver_users, "silver.silver_users")

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    logging.info("Starting transformation for SILVER.SILVER_USERS")
    transform_users()
    logging.info("Transformation completed for SILVER.SILVER_USERS")
