"""
This Airflow DAG automates the deployment of PostgreSQL schemas for the 'bronze', 'silver', and 'gold' data layers using SQL scripts.

Constants:
    DDL_ROOT (str): Root directory containing SQL scripts organized by data layer.

Functions:
    get_ddl_files(layer: str) -> list:
        Reads all `.sql` files from the specified data layer directory under DDL_ROOT.
        Returns a list of SQL statements as strings, with newline characters removed.

DAG:
    Name: hisaab_postgres_deployment
    Description: Deploys PostgreSQL schemas for 'bronze', 'silver', and 'gold' layers by executing corresponding SQL scripts.
    Schedule: Manual trigger (no schedule interval).
    Start Date: 2024-01-01
    Tags: ['postgres']
    Catchup: Disabled

Tasks:
    - deploy_bronze_schema: Executes all SQL scripts found in the 'bronze' directory.
    - deploy_silver_schema: Executes all SQL scripts found in the 'silver' directory.
    - deploy_gold_schema: Executes all SQL scripts found in the 'gold' directory.

Task Dependencies:

Notes:
    - Each task uses the PostgresOperator with dynamic task mapping to execute multiple SQL scripts in parallel.
    - The 'map_index_template' is used to extract a unique identifier from each SQL statement for task mapping.
    - Ensure that the Airflow connection 'hisaab_postgres' is properly configured.
    - SQL scripts should be organized in subdirectories under DDL_ROOT named 'bronze', 'silver', and 'gold'.
"""

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
    deploy_admin_schema = PostgresOperator.partial(
        task_id="deploy_admin_schema",
        postgres_conn_id="hisaab_postgres",
        map_index_template="{{(task.sql[task.sql.find('EXISTS')+7:task.sql.find(';')])}}",
    ).expand(sql=get_ddl_files("admin"))

    deploy_admin_dml_schema = PostgresOperator.partial(
        task_id="deploy_admin_dml_schema",
        postgres_conn_id="hisaab_postgres",
        map_index_template="{{(task.sql[task.sql.find('TRUNCATE TABLE')+14:task.sql.find(';')])}}",
    ).expand(sql=get_ddl_files("admin_dml"))

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

deploy_admin_schema >> deploy_admin_dml_schema >> [deploy_bronze_schema, deploy_silver_schema, deploy_gold_schema]
