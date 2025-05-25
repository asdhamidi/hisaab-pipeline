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
