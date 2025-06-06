import sys
from pathlib import Path
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Add the parent directory to Python's module search path
sys.path.append(str(Path(__file__).parent.parent))

from utils.utils import get_table_for_spark, write_data_to_table

def main():
    spark = None
    try:
        spark = SparkSession.builder.master("local").appName("silver_activities") .getOrCreate()
        df_src = get_table_for_spark(spark, "silver.silver_hisaab_denorm")

        df_user_monthly = df_src.groupBy("username", "year_month") \
                               .agg(F.sum("price").alias("user_monthly_spending"))

        window_spec = Window.partitionBy("year_month")
        df_result = df_user_monthly.withColumn("monthly_total_combined",
                                             F.sum("user_monthly_spending").over(window_spec))

        df_result = df_result.withColumn("user_monthly_share",
            F.round(F.col("user_monthly_spending") * 100 / F.col("monthly_total_combined"), 2))

        df_result = df_result \
            .withColumn("created_at", F.expr("current_timestamp()")) \
            .withColumn("created_by", F.expr("current_user()")) \
            .select("username", "year_month", "user_monthly_spending", "monthly_total_combined", "user_monthly_share", "created_at", "created_by")

        write_data_to_table(df_result, "gold.gold_user_monthly_share")

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
