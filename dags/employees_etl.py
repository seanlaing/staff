"""
Employees pipeline split across two DAGs with Asset-aware scheduling (Airflow 3).

1) employees_load_duckdb — loads CSV into DuckDB `raw.employees_raw` and **emits** a
   logical asset when the load task succeeds.

2) employees_dbt_transform — **scheduled on that asset**; runs dbt after each
   successful load. No time-based schedule.

Trigger `employees_load_duckdb` (manual or your own timetable). When `load_csv_to_duckdb`
succeeds, Airflow schedules a run of `employees_dbt_transform`.

Requires: duckdb, dbt-duckdb, env DUCKDB_PATH + EMPLOYEES_LOCAL_PATH (see .env.example).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
from airflow.sdk import Asset, DAG
from airflow.operators.bash import BashOperator

try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # pragma: no cover
    from airflow.operators.python import PythonOperator

# Logical URI only (no secrets). Identifies this dataset for scheduling.
EMPLOYEES_RAW_ASSET = Asset("staff://duckdb/raw/employees_raw")


def _load_csv_to_duckdb() -> None:
    csv_path = Path(os.environ["EMPLOYEES_LOCAL_PATH"]).expanduser()
    db_path = Path(os.environ["DUCKDB_PATH"]).expanduser()

    if not csv_path.is_file():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
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

# --- Producer: updates the asset when load succeeds ---
with DAG(
    dag_id="employees_load_duckdb",
    default_args=default_args,
    description="Load employees CSV into DuckDB raw (produces dataset for dbt DAG)",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["duckdb", "employees", "producer"],
) as dag_load:
    PythonOperator(
        task_id="load_csv_to_duckdb",
        python_callable=_load_csv_to_duckdb,
        outlets=[EMPLOYEES_RAW_ASSET],
    )

# --- Consumer: triggered when EMPLOYEES_RAW_ASSET is updated ---
with DAG(
    dag_id="employees_dbt_transform",
    default_args=default_args,
    description="dbt models after raw employees land (asset-triggered)",
    schedule=[EMPLOYEES_RAW_ASSET],
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["duckdb", "dbt", "employees", "consumer"],
) as dag_dbt:
    BashOperator(
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
