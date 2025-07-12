import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Add the parent directory to Python's module search path
sys.path.append(str(Path(__file__).parent.parent))

from utils.utils import get_table_for_spark, write_data_to_table

def spending_per_user_monthly():
    spark = None
    try:
        spark = SparkSession.builder.master("local").appName("silver_activities") .getOrCreate()
        df_src = get_table_for_spark(spark, "silver.silver_hisaab_denorm")

        df_expense_categories = (df_src
            .withColumn(
                "expense_category",
                F.when(F.col("price") < 100, "Small")
                 .when(F.col("price") < 200, "Standard")
                 .when(F.col("price") < 300, "Moderate")
                 .when(F.col("price") < 500, "Big")
                 .otherwise("Huge")
            )
            .groupBy(
                "expense_category", "year", "month", "week_of_month"
            )
            .agg(
                F.count("*").alias("expense_count"),
                F.min("price").alias("min_expense"),
                F.avg("price").alias("average_expense"),
                F.max("price").alias("max_expense"),
            )
            .withColumn("created_at", F.expr("current_timestamp()"))
            .withColumn("created_by", F.expr("current_user()"))
        )

        write_data_to_table(df_expense_categories, "gold.gold_expense_categories")

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    spending_per_user_monthly()
