"""
Load staff CSV into Snowflake (RAW), then run dbt staging + marts.

Requires:
- Airflow Connection `snowflake_default` (Snowflake) matching your account.
- Run `scripts/snowflake_bootstrap.sql` once in Snowflake.
- Place `employees.csv` under `data/incoming/` (mounted at `/opt/airflow/data/incoming`).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

SNOWFLAKE_CONN_ID = "snowflake_default"
DB = os.environ.get("SNOWFLAKE_DATABASE", "EMPLOYEES_ETL")
SCHEMA_RAW = os.environ.get("SNOWFLAKE_SCHEMA_RAW", "RAW")
LOCAL_CSV = os.environ.get("EMPLOYEES_LOCAL_PATH", "/opt/airflow/data/incoming/employees.csv")
STAGE = f"{DB}.{SCHEMA_RAW}.EMPLOYEES_STAGE"
FILE_FORMAT = f"{DB}.{SCHEMA_RAW}.EMPLOYEES_CSV"
RAW_TABLE = f"{DB}.{SCHEMA_RAW}.EMPLOYEES_RAW"


def _put_and_copy_sql() -> str:
    # PURGE=TRUE removes the staged file after a successful load.
    return f"""
    PUT file://{LOCAL_CSV} @{STAGE} AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
    COPY INTO {RAW_TABLE} (NAME, TITLE, OFFICE, ADDRESS, PHONE)
    FROM @{STAGE}
    FILE_FORMAT = {FILE_FORMAT}
    ON_ERROR = 'CONTINUE'
    PURGE = TRUE;
    """


default_args = {
    "owner": "data",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="employees_snowflake_dbt",
    default_args=default_args,
    description="Load employees CSV to Snowflake RAW, then dbt transform",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["snowflake", "dbt", "employees"],
) as dag:
    load_raw = SQLExecuteQueryOperator(
        task_id="put_and_copy_raw",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=_put_and_copy_sql(),
        split_statements=True,
        return_last=False,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt deps 2>/dev/null || true && "
            "dbt run --profiles-dir /opt/airflow/dbt --target dev"
        ),
        env={
            **os.environ,
            "DBT_PROFILES_DIR": "/opt/airflow/dbt",
        },
    )

    load_raw >> dbt_run
