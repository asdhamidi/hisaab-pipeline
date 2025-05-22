import os
from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator

DDL_ROOT = "./sql_scripts"


def get_ddl_files(layer):
    file_list = []
    for f in os.listdir(f"{DDL_ROOT}/{layer}"):
        if f.endswith(".sql"):
            with open(f"{DDL_ROOT}/{layer}/{f}") as f:
                file_list.append(f.read().replace("\n", ""))

    return file_list


default_args = {
    "owner": "airflow",
}

with DAG(
    "hisaab_postgres_deployment",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    tags=["postgres"],
    catchup=False,
) as dag:

    deploy_bronze_schema = PostgresOperator.partial(
        task_id="deploy_bronze_schema",
        postgres_conn_id="hisaab_postgres",
        map_index_template="{{(task.sql[task.sql.find('EXISTS')+7:task.sql.find(';')])}}",
    ).expand(sql=get_ddl_files("bronze"))

    deploy_silver_schema = PostgresOperator.partial(
        task_id="deploy_silver_schema",
        postgres_conn_id="hisaab_postgres",
        map_index_template="{{(task.sql[task.sql.find('EXISTS')+7:task.sql.find(';')])}}",
    ).expand(sql=get_ddl_files("silver"))

    deploy_gold_schema = PostgresOperator.partial(
        task_id="deploy_gold_schema",
        postgres_conn_id="hisaab_postgres",
        map_index_template="{{(task.sql[task.sql.find('EXISTS')+7:task.sql.find(';')])}}",
    ).expand(sql=get_ddl_files("gold"))

    deploy_bronze_schema >> deploy_silver_schema >> deploy_gold_schema
