import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

db_properties = {
    "driver": "org.postgresql.Driver",
    "url": "jdbc:postgresql://postgres:5432/hisaab_analytics",
    "user": "airflow",
    "password": "airflow"
}

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
        df_bronze_activities = (
            spark.read.format("jdbc")
            .option("url", "jdbc:postgresql://postgres:5432/hisaab_analytics")
            .option("dbtable", "bronze.bronze_activities")
            .option("user", "airflow")
            .option("password", "airflow")
            .option("driver", "org.postgresql.Driver")
            .load()
        )

        # Transformations
        df_silver_activities = (
            df_bronze_activities.withColumn(
                "activity_created_at",
                F.to_timestamp(F.col("activity_created_at"), "h:mm a - d/M/yy"),
            )
            .withColumn("date", F.to_date(F.col("date"), "d/M/yy"))
        )

        # Write to silver.users
        df_silver_activities.write \
            .format("jdbc") \
            .option("driver", db_properties["driver"]) \
            .option("url", db_properties["url"]) \
            .option("dbtable", "silver.silver_activities") \
            .option("user", db_properties["user"]) \
            .option("password", db_properties["password"]) \
            .option("truncate", "true") \
            .mode("overwrite") \
            .save()

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
