from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

def transform_users():
    spark = SparkSession.builder \
        .appName("BronzeToSilver_Users") \
        .config("spark.jars", "/opt/airflow/jars/postgresql-42.5.4.jar") \
        .getOrCreate()

    # Read from bronze.users
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/hisaab_analytics") \
        .option("dbtable", "bronze.users") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .load()

    # Transformations
    df_silver = df.withColumn(
        "created_at", 
        to_timestamp(col("created_at"), "dd-MM-yyyy hh:mm a")
    ).drop("password")  # Remove sensitive data

    # Write to silver.users
    df_silver.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/hisaab_analytics") \
        .option("dbtable", "silver.users") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .mode("overwrite") \
        .save()

if __name__ == "__main__":
    transform_users()