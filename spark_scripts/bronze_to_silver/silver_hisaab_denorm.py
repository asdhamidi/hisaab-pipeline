from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

def create_denormalized():
    spark = SparkSession.builder \
        .appName("SilverToGold_Denormalized") \
        .getOrCreate()

    df_users = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/hisaab_analytics") \
        .option("dbtable", "bronze.users") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .load()
    
    df_entries = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/hisaab_analytics") \
        .option("dbtable", "bronze.entries") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .load()
    
    df_activities = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/hisaab_analytics") \
        .option("dbtable", "bronze.activities") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .load()

    df_gold = df_entries.join(
        df_users, 
        df_entries.paid_by == df_users.username, 
        "left"
    ).join(
        df_activities,
        df_entries._id == df_activities.entry_id,
        "left"
    ).select(
        df_entries["*"],
        df_users["admin"].alias("payer_is_admin"),
        df_activities["activity_type"]
    )

    df_gold.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/hisaab_analytics") \
        .option("dbtable", "silver.hisaab_denorm") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .mode("overwrite") \
        .save()


if __name__ == "__main__":
    create_denormalized()