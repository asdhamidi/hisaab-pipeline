from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys
from pathlib import Path

# Add the parent directory to Python's module search path
sys.path.append(str(Path(__file__).parent.parent))

from utils.utils import get_table_for_spark, write_data_to_table

def create_denormalized():
    spark = SparkSession.builder \
        .master("local") \
        .appName("SilverToGold_Denormalized") \
        .getOrCreate()

    drop_cols = ["created_at", "created_by"]
    silver_users = get_table_for_spark(spark, "silver.silver_users").drop(*drop_cols)
    silver_entries = get_table_for_spark(spark, "silver.silver_entries").drop(*drop_cols)
    silver_activities = get_table_for_spark(spark, "silver.silver_activities").drop(*drop_cols)

    # Part 1: Updated entries
    select_cols = [
        "u.username",
        "e.date",
        "e.items",
        "e.paid_by",
        "e.notes",
        "e.price",
        "e.owed_all",
        "e.owed_by",
        "e.previous_versions",
        "e.entry_updated_at",
        "e.entry_created_at",
        "a.activity",
        "a.activity_created_at",
        "u.admin",
        "u.user_created_at",
        "created_at",
        "created_by"
    ]

    updated_entries = silver_entries.alias("e") \
        .join(
            silver_activities.alias("a"),
                (F.col("e.entry_updated_at") == F.col("a.activity_created_at")) &
                (F.col("e.paid_by") == F.col("a.username")) &
                (F.lower(F.trim(F.expr(
                    "substring(a.activity, position('for' in a.activity)+4, length(a.activity))"
                ))) == F.lower(F.trim(F.col("e.items")))
            ),
            "left"
        ) \
        .join(
            silver_users.alias("u"),
            F.col("u.username") == F.col("e.paid_by"),
            "left"
        ) \
        .filter(F.col("e.entry_updated_at").isNotNull()) \
        .filter(F.col("a.activity").ilike("%updated%")) \
        .withColumn("created_at", F.expr("current_timestamp()")) \
        .withColumn("created_by", F.expr("current_user()")) \
        .select(*select_cols)

    non_updated_entries = silver_entries.alias("e") \
        .join(
            silver_activities.alias("a"),
                (F.col("e.entry_created_at") == F.col("a.activity_created_at")) &
                (F.col("e.paid_by") == F.col("a.username")) &
                (F.lower(F.trim(F.expr(
                    "substring(a.activity, position('for' in a.activity)+4, length(a.activity))"
                ))) == F.lower(F.trim(F.col("e.items")))
            ),
            "left"
        ) \
        .join(
            silver_users.alias("u"),
            F.col("u.username") == F.col("e.paid_by"),
            "left"
        ) \
        .filter(F.col("e.entry_updated_at").isNull()) \
        .filter(F.col("a.activity").ilike("%created%")) \
        .withColumn("created_at", F.expr("current_timestamp()")) \
        .withColumn("created_by", F.expr("current_user()")) \
        .select(*select_cols)

    min_date = silver_activities.agg(F.min("date")).collect()[0][0] or "2024-09-01"
    non_activities_entries = silver_entries.alias("e") \
            .join(
                silver_activities.alias("a"),
                    (F.col("e.entry_created_at") == F.col("a.activity_created_at")) &
                    (F.col("e.paid_by") == F.col("a.username")) &
                    (F.lower(F.trim(F.expr(
                        "substring(a.activity, position('for' in a.activity)+4, length(a.activity))"
                    ))) == F.lower(F.trim(F.col("e.items")))
                ),
                "left"
            ) \
            .join(
                silver_users.alias("u"),
                F.col("u.username") == F.col("e.paid_by"),
                "left"
            ) \
            .filter(F.col("e.date") < min_date) \
        .withColumn("created_at", F.expr("current_timestamp()")) \
        .withColumn("created_by", F.expr("current_user()")) \
        .select(*select_cols)

    silver_hisaab_denorm = updated_entries \
        .union(non_updated_entries) \
        .union(non_activities_entries)

    silver_hisaab_denorm = silver_hisaab_denorm.distinct()

    write_data_to_table(silver_hisaab_denorm, "silver.silver_hisaab_denorm")


if __name__ == "__main__":
    create_denormalized()
