import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).parent.parent))

from utils.utils import get_table_for_spark, write_data_to_table

def user_net_balance():
    spark = None
    try:
        spark = (
            SparkSession.builder.master("local")
            .appName("silver_activities")
            .getOrCreate()
        )
        df_activities = get_table_for_spark(spark, "silver.silver_activities")
        dim_time = get_table_for_spark(spark, "silver.silver_time_dim")

        df_activity_metric = (
            df_activities.alias("a")
            .join(
                dim_time.alias("t"),
                ["date"]
            )
            .withColumn(
                "action_type",
                F.expr("TRIM(SUBSTRING(a.activity, 0, position(' ' in a.activity)))")
            )
            .groupBy(
                ["t.week_of_month", "t.year", "t.month", "a.username"]
            )
            .agg(
                F.countDistinct("date").alias("active_days"),
                F.count("*").alias("activity_count"),
                F.sum(F.when(F.col("action_type").isin("opened", "logged"), 1).otherwise(0)).alias("visit_count"),
                F.sum(F.when(F.col("action_type") == "created", 1).otherwise(0)).alias("create_count"),
                F.sum(F.when(F.col("action_type").isin("updated", "deleted"), 1).otherwise(0)).alias("modification_count")
            )
            .withColumn("created_at", F.expr("current_timestamp()"))
            .withColumn("created_by", F.expr("current_user()"))
        )

        write_data_to_table(df_activity_metric, "gold.gold_user_engagement")

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    user_net_balance()
