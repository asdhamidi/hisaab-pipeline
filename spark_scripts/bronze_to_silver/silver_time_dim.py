import sys
from pathlib import Path
# Add the parent directory to Python's module search path
sys.path.append(str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StructType, StructField
from pandas import date_range
from datetime import datetime
from utils.utils import write_data_to_table

def create_time_dimension_table():
    # Initialize Spark session
    spark = SparkSession.builder.master("local").appName("TimeDimensionCreation").getOrCreate()

    # Create a DataFrame with all dates in the range
    days = (datetime(2040, 1, 1) - datetime(2024, 1, 1)).days

    # Create DataFrame with sequence of integers (cast to int)
    dates_df = spark.range(days).select(
        F.expr("date_add('2024-01-01', cast(id as int))").alias("date")
    )

    # Add all time dimension attributes
    time_dim_df = dates_df \
        .withColumn("day_of_month", F.dayofmonth("date")) \
        .withColumn("day_of_week", F.dayofweek("date")) \
        .withColumn("day_name", F.date_format("date", "EEEE")) \
        .withColumn("day_short_name", F.date_format("date", "EEE")) \
        .withColumn("is_weekend", F.when(F.dayofweek("date").isin(1, 7), True).otherwise(False)) \
        .withColumn("week_of_month", F.ceil(F.dayofmonth("date") / 7).cast("int")) \
        .withColumn("week_of_year", F.weekofyear("date")) \
        .withColumn("month", F.month("date")) \
        .withColumn("month_name", F.date_format("date", "MMMM")) \
        .withColumn("month_short_name", F.date_format("date", "MMM")) \
        .withColumn("quarter", F.quarter("date")) \
        .withColumn("year", F.year("date")) \
        .withColumn("year_month", F.date_format("date", "yyyy-MM")) \
        .withColumn("year_quarter", F.concat(F.year("date"), F.lit("-Q"), F.quarter("date"))) \
        .withColumn("month_third",
                    F.when(F.dayofmonth("date") <= 10, "First")
                     .when(F.dayofmonth("date") <= 20, "Second")
                     .otherwise("Third")) \
        .withColumn("is_leap_year",
                    F.when((F.year("date") % 400 == 0) | ((F.year("date") % 100 != 0) & (F.year("date") % 4 == 0)), True)
                     .otherwise(False)) \
        .withColumn("day_of_year", F.dayofyear("date")) \
        .withColumn("first_day_of_month", F.date_trunc("month", "date")) \
        .withColumn("last_day_of_month", F.last_day("date")) \
        .withColumn("days_in_month", F.dayofmonth(F.last_day("date"))) \
        .withColumn("is_first_day_of_month", F.col("date") == F.col("first_day_of_month")) \
        .withColumn("is_last_day_of_month", F.col("date") == F.col("last_day_of_month"))

    # Reorder columns with date_key first
    write_data_to_table(time_dim_df, "silver.silver_time_dim")

if __name__ == "__main__":
    create_time_dimension_table()
