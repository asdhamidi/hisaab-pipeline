import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Add the parent directory to Python's module search path
from itertools import groupby

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
        df_src = get_table_for_spark(spark, "gold.gold_debt_ledger")

        net_balance = df_src \
            .groupby(
                F.col("creditor").alias("username"),
                F.col("year_month")
            ) \
            .agg(
                F.sum("net_balance").alias("net_balance")
            ) \
            .withColumn("created_at", F.current_timestamp()) \
            .withColumn("created_by", F.expr("current_user()"))

        write_data_to_table(net_balance, "gold.gold_user_net_balances")

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    user_net_balance()
