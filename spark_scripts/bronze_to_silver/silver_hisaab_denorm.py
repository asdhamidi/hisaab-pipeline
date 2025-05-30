from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys
from pathlib import Path

# Add the parent directory to Python's module search path
sys.path.append(str(Path(__file__).parent.parent))

from utils.utils import get_table_for_spark, write_data_to_table

def create_denormalized():
    """
    Creates a denormalized 'silver_hisaab_denorm' table by joining and transforming user, entry, and activity data.

    This function performs the following steps:
        1. Initializes a Spark session for data processing.
        2. Loads user, entry, and activity tables from the 'silver' schema, dropping audit columns.
        3. Constructs three DataFrames:
            - `updated_entries`: Entries that have been updated, joined with corresponding activities and user details.
            - `non_updated_entries`: Entries that have not been updated, joined with corresponding activities and user details.
            - `non_activities_entries`: Entries with dates earlier than the minimum activity date, joined with user details.
        4. Applies necessary filters and transformations, including:
            - Matching entries and activities based on timestamps, usernames, and item descriptions.
            - Filtering for updated or created activities as appropriate.
            - Adding audit columns (`created_at`, `created_by`) with current timestamp and user.
        5. Unions the three DataFrames, removes duplicates, and writes the result to the 'silver.silver_hisaab_denorm' table.

    - The denormalized table provides a unified view of user, entry, and activity data for downstream analytics or reporting.

    Raises:
        Exception: If any of the required tables are missing or if Spark encounters an error during processing.

    Returns:
        None
    """
    spark = SparkSession.builder \
        .master("local") \
        .appName("SilverToGold_Denormalized") \
        .getOrCreate()

    drop_cols = ["created_at", "created_by"]
    silver_users = get_table_for_spark(spark, "silver.silver_users").drop(*drop_cols)
    silver_entries = get_table_for_spark(spark, "silver.silver_entries").drop(*drop_cols)
    silver_activities = get_table_for_spark(spark, "silver.silver_activities").drop(*drop_cols)
    silver_time_dim = get_table_for_spark(spark, "silver.silver_time_dim")

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
        "t.day_of_month",
        "t.day_of_week",
        "t.day_name",
        "t.day_short_name",
        "t.is_weekend",
        "t.week_of_month",
        "t.week_of_year",
        "t.month",
        "t.month_name",
        "t.month_short_name",
        "t.quarter",
        "t.year",
        "t.year_month",
        "t.year_quarter",
        "t.month_third",
        "t.is_leap_year",
        "t.day_of_year",
        "t.first_day_of_month",
        "t.last_day_of_month",
        "t.days_in_month",
        "t.is_first_day_of_month",
        "t.is_last_day_of_month",
        "created_at",
        "created_by"
    ]

    # Part 1: Updated entries
    items_activity_match_str = "substring(a.activity, position('for' in a.activity)+4, length(a.activity))"
    updated_entries = silver_entries.alias("e") \
        .join(
            silver_activities.alias("a"),
                (F.col("e.entry_updated_at") == F.col("a.activity_created_at")) &
                (F.col("e.paid_by") == F.col("a.username")) &
                (F.lower(F.trim(F.expr(items_activity_match_str))) == F.lower(F.trim(F.col("e.items")))
            ),
            "left"
        )\
        .filter(F.col("e.entry_updated_at").isNotNull()) \
        .filter(F.col("a.activity").ilike("%updated%")) \


    non_updated_entries = silver_entries.alias("e") \
        .join(
            silver_activities.alias("a"),
                (F.col("e.entry_created_at") == F.col("a.activity_created_at")) &
                (F.col("e.paid_by") == F.col("a.username")) &
                (F.lower(F.trim(F.expr(items_activity_match_str))) == F.lower(F.trim(F.col("e.items")))
            ),
            "left"
        ) \
         \
        .filter(F.col("e.entry_updated_at").isNull()) \
        .filter(F.col("a.activity").ilike("%created%"))

    min_date = silver_activities.agg(F.min("date")).collect()[0][0] or "2024-09-01"
    non_activities_entries = silver_entries.alias("e") \
        .join(
            silver_activities.alias("a"),
                (F.col("e.entry_created_at") == F.col("a.activity_created_at")) &
                (F.col("e.paid_by") == F.col("a.username")) &
                (F.lower(F.trim(F.expr(items_activity_match_str))) == F.lower(F.trim(F.col("e.items")))
            ),
            "left"
        ) \
        .filter(F.col("e.date") < min_date)

    silver_hisaab_denorm = updated_entries \
        .union(non_updated_entries) \
        .union(non_activities_entries)

    silver_hisaab_denorm = silver_hisaab_denorm \
        .join(
            silver_users.alias("u"),
            F.col("u.username") == F.col("e.paid_by"),
            "left"
        ) \
        .join(
            silver_time_dim.alias("t"),
            F.col("t.date") == F.col("e.date"),
            "left"
        ) \
        .withColumn("created_at", F.expr("current_timestamp()")) \
        .withColumn("created_by", F.expr("current_user()")) \
        .distinct() \
        .select(*select_cols)

    write_data_to_table(silver_hisaab_denorm, "silver.silver_hisaab_denorm")


if __name__ == "__main__":
    create_denormalized()
