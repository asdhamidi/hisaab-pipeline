import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Add the parent directory to Python's module search path

sys.path.append(str(Path(__file__).parent.parent))

from utils.utils import get_table_for_spark, write_data_to_table


def debt_ledger():
    spark = None
    try:
        spark = (
            SparkSession.builder.master("local")
            .appName("silver_activities")
            .getOrCreate()
        )

        df_src = get_table_for_spark(spark, "silver.silver_hisaab_denorm")

        debt = df_src \
            .withColumn(
                "owed_by_list",
                    F.split(
                        F.regexp_replace(
                            F.col("owed_by"),
                            r"^\[|\]$",
                            ""),
                    ",")
                ) \
            .withColumn(
                "num_debtors",
                    F.size(
                        F.col("owed_by_list")
                    )
            ) \
            .withColumn("share", F.col("price") / F.col("num_debtors")) \
            .withColumn("debtor", F.explode("owed_by_list")) \
            .withColumn("debtor", F.trim(F.col("debtor"))) \
            .filter(F.col("debtor") != F.col("paid_by")) \
            .groupBy(
                "debtor",
                F.col("paid_by").alias("creditor"),
                "year_month") \
            .agg(
                F.sum("share").alias("amount")
            ) \
            .filter(F.col("creditor") != F.col("debtor"))

        debt = debt.alias("left").join(
                debt.alias("right"),
                ((F.col("left.year_month") == F.col("right.year_month")) &
                (F.col("left.debtor") == F.col("right.creditor")) &
                (F.col("left.creditor") == F.col("right.debtor"))),
                "left_outer"
        )

        debt = debt \
            .withColumn(
                "net_balance",
                F.when(
                    F.col("left.amount").isNull(),
                    -F.col("right.amount")  # Only right exists (B → A)
                ).when(
                    F.col("right.amount").isNull(),
                    F.col("left.amount")  # Only left exists (A → B)
                ).otherwise(
                    F.col("left.amount") - F.col("right.amount")  # Both exist (net balance)
                )
            ) \
            .filter(F.col("left.creditor") != F.col("left.debtor")) \
            .select(
                F.col("left.debtor").alias("debtor"),
                F.col("left.creditor").alias("creditor"),
                F.col("left.year_month").alias("year_month"),
                "net_balance"
            ) \
            .withColumn("created_at", F.current_timestamp()) \
            .withColumn("created_by", F.expr("current_user()"))

        write_data_to_table(debt, "gold.gold_debt_ledger")

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    debt_ledger()
