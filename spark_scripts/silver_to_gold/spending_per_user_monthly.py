import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys
from pathlib import Path

# Add the parent directory to Python's module search path
sys.path.append(str(Path(__file__).parent.parent))

from utils.utils import get_table_for_spark, write_data_to_table

def transform_activities():
    spark = None
    try:
        spark = SparkSession.builder.master("local").appName("silver_activities") .getOrCreate()
        df_src = get_table_for_spark(spark, "silver.silver_hisaab_denorm")

        df_spending_per_user_monthly = df_src \
        .groupBy(["username", "month_name", "year"]) \
        .agg(F.sum("price").alias("total_spending")) \
        .withColumn("created_at", F.expr("current_timestamp()")) \
        .withColumn("created_by", F.expr("current_user()"))

        write_data_to_table(df_spending_per_user_monthly, "gold.gold_spending_per_user_monthly")

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    logging.info("Starting transformation for SILVER.SILVER_ACTIVITIES")
    transform_activities()
    logging.info("Transformation completed for SILVER.SILVER_ACTIVITIES")
