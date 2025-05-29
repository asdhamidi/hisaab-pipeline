def get_table_for_spark(sparkSession, table_name):
    return sparkSession.read.format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/hisaab_analytics") \
        .option("dbtable", table_name) \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .option("driver", "org.postgresql.Driver") \
        .load()



def write_data_to_table(table_dataframe, target_table_name):
    db_properties = {
        "driver": "org.postgresql.Driver",
        "url": "jdbc:postgresql://postgres:5432/hisaab_analytics",
        "user": "airflow",
        "password": "airflow"
    }

    return table_dataframe.write \
        .format("jdbc") \
        .option("driver", db_properties["driver"]) \
        .option("url", db_properties["url"]) \
        .option("dbtable", target_table_name) \
        .option("user", db_properties["user"]) \
        .option("password", db_properties["password"]) \
        .option("truncate", "true") \
        .mode("overwrite") \
        .save()
