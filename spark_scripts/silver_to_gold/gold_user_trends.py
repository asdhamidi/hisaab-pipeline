import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import Window
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
        df_src = get_table_for_spark(spark, "gold.gold_debt_ledger")
        df_denorm = get_table_for_spark(spark, "silver.silver_hisaab_denorm")

        net_credit = (
            df_src.filter(F.col("net_balance") > 0)
            .groupby("creditor", "year_month")
            .agg(F.sum("net_balance").alias("total_credit"))
            .withColumnRenamed("creditor", "username")
        )

        net_debt = (
            df_src.filter(F.col("net_balance") < 0)
            .groupby("creditor", "year_month")
            .agg(F.sum("net_balance").alias("total_debt"))
            .withColumnRenamed("creditor", "username")
        )

        net_spending = df_denorm.groupby("username", "year_month").agg(
            F.sum("price").alias("total_spend")
        )

        result = (
            net_spending.join(net_credit, ["username", "year_month"], "left_outer")
            .join(net_debt, ["username", "year_month"], "left_outer")
            .fillna({"total_credit": 0, "total_debt": 0})
        )

        window = Window.partitionBy("username").orderBy("year_month")
        result = (
            result
            .withColumn(
                "mom_net_spend_change",
                F.round(
                    (F.col("total_spend") - F.lag("total_spend").over(window)) / 100,
                    2
                )
            )
            .withColumn(
                "mom_net_credited_change",
                F.round(
                    (F.col("total_credit") - F.lag("total_credit").over(window)) / 100,
                    2
                )
            )
            .withColumn(
                "mom_net_debted_change",
                F.round(
                    (F.col("total_debt") - F.lag("total_debt").over(window)) / 100,
                    2
                )
            )
            .filter(F.col("mom_net_spend_change").isNotNull())
            .select(
                "username",
                "year_month",
                "mom_net_spend_change",
                "mom_net_credited_change",
                "mom_net_debted_change",
                F.current_timestamp().alias("created_at"),
                F.expr("current_user()").alias("created_by")
            )
        )


        write_data_to_table(result, "gold.gold_user_trends")

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    user_net_balance()
