import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
import sys
from pathlib import Path

# Add the parent directory to Python's module search path
sys.path.append(str(Path(__file__).parent.parent))

from utils.utils import get_table_for_spark, write_data_to_table

def transform_activities():
    """
    Reads activity data from the 'bronze.bronze_activities' table in a PostgreSQL database,
    applies necessary transformations, and writes the processed data to the 'silver.silver_activities' table.

    Steps performed:
    1. Establishes a Spark session.
    2. Reads the raw activities data from the 'bronze.bronze_activities' table using JDBC.
    3. Transforms the data:
        - Converts the 'activity_created_at' column from string to timestamp using the format "h:mm a - d/M/yy".
        - Converts the 'date' column from string to date using the format "d/M/yy".
    4. Writes the transformed data to the 'silver.silver_activities' table in the same PostgreSQL database,
       overwriting any existing data in the target table.
    5. Handles exceptions by printing and re-raising them.
    6. Ensures the Spark session is stopped after execution.

    Raises:
        Exception: If any error occurs during the ETL process.
    """
    spark = None
    try:
        spark = SparkSession.builder.master("local").getOrCreate()

        # Read from bronze.users
        df_bronze_activities = get_table_for_spark(spark, "bronze.bronze_activities")


        # Transformations
        df_silver_activities = (
            df_bronze_activities.withColumn(
                "activity_created_at",
                F.to_timestamp(F.col("activity_created_at"), "h:mm a - d/M/yy"),
            )
            .withColumn("date", F.to_date(F.col("date"), "d/M/yy"))
            .dropDuplicates()
        )

        # Write to silver.users
        write_data_to_table(df_silver_activities, "silver.silver_activities")

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    logging.info("Starting transformation for BRONZE.BRONZE_ACTIVITIES")
    transform_activities()
    logging.info("Transformation completed for BRONZE.BRONZE_ACTIVITIES")
