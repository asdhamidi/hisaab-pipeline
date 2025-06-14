import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Add the parent directory to Python's module search path

sys.path.append(str(Path(__file__).parent.parent))

from utils.utils import get_table_for_spark, write_data_to_table


def expense_stats():
    spark = None
    try:
        spark = (
            SparkSession.builder.master("local")
            .appName("silver_activities")
            .getOrCreate()
        )
        df_src = get_table_for_spark(spark, "silver.silver_hisaab_denorm")

        df_with_expense_type = df_src.withColumn(
            "expense_type",
            F.when(
                F.col("owed_all"),
                F.lit("all-shared")
            )
            .when(
                (
                    F.size(
                        F.split(F.regexp_replace(F.col("owed_by"), r"^\[|\]$", ""), ",")
                    ) == 1
                )
                | (
                    (F.size(
                        F.split(F.regexp_replace(F.col("owed_by"), r"^\[|\]$", ""), ",")
                    ) == 2)
                    & F.array_contains(
                        F.split(
                            F.regexp_replace(F.col("owed_by"), r"^\[|\]$", ""), ","
                        ),
                        F.col("paid_by"),
                    )
                ),
                F.lit("one-on-one-shared"),
            )
            .otherwise(F.lit("semi-shared")),
        )


        df_agg = (
            df_with_expense_type.groupby("expense_type", "year_month")
            .agg(
                F.min("price").alias("min_amount"),
                F.avg("price").alias("avg_amount"),
                F.max("price").alias("max_amount"),
                F.sum("price").alias("total_amount"),
                F.count("*").alias("no_of_entries"),
            )
            .withColumn("created_at", F.current_timestamp())
            .withColumn("created_by", F.expr("current_user()"))
        )

        write_data_to_table(df_agg, "gold.gold_expense_stats")

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    expense_stats()
