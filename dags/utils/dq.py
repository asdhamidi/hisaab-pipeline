import logging
from typing import List, Dict, Any
from datetime import datetime, timezone
from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook

def insert_dq_results(hook: PostgresHook, dq_results: List[Dict[str, Any]]) -> None:
    """Insert DQ results into the tracking table"""
    if not dq_results:
        return

    insert_query = """
        INSERT INTO admin.admin_dq_results
        (id, result, execution_date, comments)
        VALUES (%s, %s, %s, %s)
    """

    records = [
        (r['id'], r['result'], r['execution_date'], r['comments'])
        for r in dq_results
    ]

    try:
        hook.run(insert_query, parameters=records)
    except Exception as e:
        raise AirflowException(f"Failed to insert DQ results: {str(e)}")

def run_dq_checks(table_schema: str, table_name: str) -> None:
    """
    Executes all DQ checks defined for a specific table
    Returns list of results ready for insertion into ADMIN_DQ_RESULTS
    """
    pg_hook = PostgresHook(postgres_conn_id='your_postgres_conn_id')
    execution_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f%z")[:-2]
    results = []

    # Retrieve configured checks for this table
    retrieval_query = f"""
        SELECT id, type, target_column, range_check, lower_range, upper_range,
               is_null_check, is_unique_check, is_duplicate_check
        FROM admin.admin_dq_checks
        WHERE target_schema = '{table_schema}'
          AND target_table = '{table_name}'
          AND active = true
    """

    try:
        checks = pg_hook.get_records(retrieval_query)
        failed_flag = 0

        for check in checks:
            check_id, check_type, column, range_check, lower, upper, null_check, unique_check, dup_check = check

            if range_check:
                result = _execute_range_check(pg_hook, table_schema, table_name, column, lower, upper)
            elif check_type=="NULL":
                result = _execute_null_check(pg_hook, table_schema, table_name, column)
            elif check_type=="UNIQUE":
                result = _execute_unique_check(pg_hook, table_schema, table_name, column)
            elif check_type=="DUPLICATE":
                result = _execute_duplicate_check(pg_hook, table_schema, table_name, column)
            else:
                result = "UNSUPPORTED_CHECK_TYPE"

            if result != "SUCCESS":
                failed_flag = 1
                print(f"Check {check_id} failed")

            results.append({
                'id': check_id,
                'result': result,
                'execution_date': execution_date,
                'comments': f"{check_type} check on {table_schema}.{table_name}.{column}"
            })

    except Exception as e:
        raise AirflowException(f"DQ check failed: {str(e)}")

    insert_dq_results(pg_hook, results)

    if failed_flag:
        raise AirflowException(f"DQ checks failed for {table_schema}.{table_name}")

def _execute_range_check(hook: PostgresHook, schema: str, table: str, column: str,
                        lower: float, upper: float) -> str:
    """Check if column values are within specified range"""
    query = f"""
        SELECT COUNT(*)
        FROM {schema}.{table}
        WHERE {column} < {lower} OR {column} > {upper}
    """
    invalid_count = hook.get_first(query)[0]
    return f"RANGE_VIOLATION ({invalid_count} records)" if invalid_count > 0 else "SUCCESS"

def _execute_null_check(hook: PostgresHook, schema: str, table: str, column: str) -> str:
    """Check for null values in non-nullable columns"""
    query = f"""
        SELECT COUNT(*)
        FROM {schema}.{table}
        WHERE {column} IS NULL
    """
    null_count = hook.get_first(query)[0]
    return f"NULL_VIOLATION ({null_count} records)" if null_count > 0 else "SUCCESS"

def _execute_unique_check(hook: PostgresHook, schema: str, table: str, column: str) -> str:
    """Check if column values are unique"""
    query = f"""
        SELECT COUNT(*) - COUNT(DISTINCT {column})
        FROM {schema}.{table}
    """
    duplicate_count = hook.get_first(query)[0]
    return f"UNIQUENESS_VIOLATION ({duplicate_count} duplicates)" if duplicate_count > 0 else "SUCCESS"

def _execute_duplicate_check(hook: PostgresHook, schema: str, table: str, column: str) -> str:
    """Check for duplicate rows based on specified column"""
    query = f"""
        SELECT COUNT(*)
        FROM (
            SELECT {', '.join(column)}, COUNT(*)
            FROM {schema}.{table}
            GROUP BY {', '.join(column)}
            HAVING COUNT(*) > 1
        ) AS duplicates
    """
    duplicate_count = hook.get_first(query)[0]
    return f"DUPLICATE_ROWS ({duplicate_count} groups)" if duplicate_count > 0 else "SUCCESS"
