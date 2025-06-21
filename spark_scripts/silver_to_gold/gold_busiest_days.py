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

        df_activities = (df_activities
            .join(
                dim_time,
                df_activities.date == dim_time.date
            )
            .groupby("day_of_month")
            .agg(
                F.count("*").alias("activity_count")
            )
            .withColumn("created_at", F.expr("current_timestamp()"))
            .withColumn("created_by", F.expr("current_user()"))
        )

        write_data_to_table(df_activities, "gold.gold_busiest_days")

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    user_net_balance()
