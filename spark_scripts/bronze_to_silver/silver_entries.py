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


def transform_entries():
    """
    Reads entry data from the 'bronze.bronze_entries' table in a PostgreSQL database,
    applies a series of transformations to clean and standardize the data, and writes
    the transformed data to the 'silver.silver_entries' table.

    Steps performed:
    1. Establishes a Spark session.
    2. Reads the source data from the 'bronze.bronze_entries' table using JDBC.
    3. Applies the following transformations:
        - Converts 'entry_created_at' from string to timestamp using the format "h:mm a - d/M/yy".
        - Converts 'date' from string to date using the format "d/M/yy".
        - Casts 'price' to DecimalType(5, 2).
        - Casts 'owed_all' to boolean.
        - Parses 'updated_at' as timestamp if it matches a datetime pattern; otherwise,
          appends "12:00 AM" and parses as date with time.
    4. Writes the transformed DataFrame to the 'silver.silver_entries' table in PostgreSQL,
       overwriting existing data.
    5. Handles exceptions and ensures the Spark session is stopped.

    Raises:
        Exception: If any error occurs during the transformation or writing process.
    """
    spark = None
    try:
        spark = SparkSession.builder.master("local").getOrCreate()

        # Read from bronze.users
        df_bronze_entries = (
            spark.read.format("jdbc")
            .option("url", "jdbc:postgresql://postgres:5432/hisaab_analytics")
            .option("dbtable", "bronze.bronze_entries")
            .option("user", "airflow")
            .option("password", "airflow")
            .option("driver", "org.postgresql.Driver")
            .load()
        )

        # Transformations
        df_silver_entries = (
            df_bronze_entries.withColumn(
                "entry_created_at",
                F.to_timestamp(F.col("entry_created_at"), "h:mm a - d/M/yy"),
            )
            .withColumn("date", F.to_date(F.col("date"), "d/M/yy"))
            .withColumn("price", F.col("price").cast(DecimalType(5, 2)))
            .withColumn("owed_all", F.col("owed_all").cast("boolean"))
            .withColumn(
                "updated_at",
                F.when(
                    F.col("updated_at").rlike(
                        r"\d{1,2}:\d{2} [AP]M - \d{1,2}/\d{1,2}/\d{2}"
                    ),  # datetime pattern
                    F.to_timestamp("updated_at", "h:mm a - M/d/yy"),
                ).otherwise(
                    F.to_timestamp(
                        F.concat("updated_at", F.lit(
                            " 12:00 AM")), "d/M/yy h:mm a"
                    )  # date-only pattern
                ),
            )
        )

        # Write to silver.users
        df_silver_entries.write \
            .format("jdbc") \
            .option("driver", db_properties["driver"]) \
            .option("url", db_properties["url"]) \
            .option("dbtable", "silver.silver_entries") \
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
    logging.info("Starting transformation for BRONZE.BRONZE_ENTRIES")
    transform_entries()
    logging.info("Transformation completed for BRONZE.BRONZE_ENTRIES")
