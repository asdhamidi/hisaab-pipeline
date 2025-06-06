import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Add the parent directory to Python's module search path
from itertools import groupby
sys.path.append(str(Path(__file__).parent.parent))

from utils.utils import get_table_for_spark, write_data_to_table

def main():
    spark = None
    try:
        spark = SparkSession.builder.master("local").appName("silver_activities") .getOrCreate()
        df_src = get_table_for_spark(spark, "silver.silver_hisaab_denorm")

        df_combined = df_src \
            .groupBy("username", "year_month") \
            .agg(
                F.count_distinct("date", "username", "items", "price").alias("entry_count"),
                F.avg("price").alias("avg_price"),
                F.median("price").alias("median_price"),
            ) \
            .withColumn("created_at", F.expr("current_timestamp()")) \
            .withColumn("created_by", F.expr("current_user()"))

        write_data_to_table(df_combined, "gold.gold_user_entry_frequency")

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
