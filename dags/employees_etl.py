"""
Load staff CSV into a local DuckDB file, then run dbt staging + marts.

Requires:
- `duckdb` and `dbt-duckdb` in the Airflow image (see docker/airflow/requirements.txt).
- Env `DUCKDB_PATH` (database file) and `EMPLOYEES_LOCAL_PATH` (CSV).
- With Docker Compose, `./data` is mounted at `/opt/airflow/data`.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
from airflow import DAG
from airflow.operators.bash import BashOperator

try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # pragma: no cover
    from airflow.operators.python import PythonOperator


def _load_csv_to_duckdb() -> None:
    csv_path = Path(os.environ["EMPLOYEES_LOCAL_PATH"]).expanduser()
    db_path = Path(os.environ["DUCKDB_PATH"]).expanduser()

    if not csv_path.is_file():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        # Header columns match employees.csv: Name, Title, Office, Address, Phone
        con.execute(
            """
            CREATE OR REPLACE TABLE raw.employees_raw AS
            SELECT
                CAST("Name" AS VARCHAR) AS name,
                CAST("Title" AS VARCHAR) AS title,
                CAST("Office" AS VARCHAR) AS office,
                CAST("Address" AS VARCHAR) AS address,
                CAST("Phone" AS VARCHAR) AS phone
            FROM read_csv_auto(?, header = true)
            """,
            [str(csv_path)],
        )
    finally:
        con.close()


default_args = {
    "owner": "data",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="employees_duckdb_dbt",
    default_args=default_args,
    description="Load employees CSV into DuckDB, then dbt transform",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["duckdb", "dbt", "employees"],
) as dag:
    load_raw = PythonOperator(
        task_id="load_csv_to_duckdb",
        python_callable=_load_csv_to_duckdb,
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
