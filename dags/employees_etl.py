"""
Employees pipeline split across two DAGs with Asset-aware scheduling (Airflow 3).

1) employees_load_duckdb — fetches House directory data from `https://directory.house.gov/`,
   loads it into DuckDB `raw.employees_raw`, and **emits** a logical asset when the load task
   succeeds.

2) employees_dbt_transform — **scheduled on that asset**; runs dbt after each
   successful load. No time-based schedule.

Trigger `employees_load_duckdb` (manual or your own timetable). When `load_to_duckdb`
succeeds, Airflow schedules a run of `employees_dbt_transform`.

Requires: duckdb, dbt-duckdb, env DUCKDB_PATH (see .env.example). Optional DIRECTORY_URL.
"""

from __future__ import annotations

import os
import json
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
from airflow.sdk import Asset, DAG

try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:  # pragma: no cover
    from airflow.operators.bash import BashOperator

try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # pragma: no cover
    from airflow.operators.python import PythonOperator

# Logical URI only (no secrets). Identifies this dataset for scheduling.
EMPLOYEES_RAW_ASSET = Asset("staff://duckdb/raw/employees_raw")


def _extract_embedded_value_array(html: str, value_name: str) -> list[dict]:
    """
    directory.house.gov embeds the whole dataset in inline scripts:
      angular.module(...).value("employees", [ ... ]);
      angular.module(...).value("offices", [ ... ]);

    The payload is JSON-compatible (double-quoted keys, null, nested objects).
    """
    marker = f'.value("{value_name}",'
    start_idx = html.find(marker)
    if start_idx < 0:
        raise ValueError(f'Could not find marker for value "{value_name}"')

    # Find the first '[' after the marker, then scan to the matching ']'.
    bracket_start = html.find("[", start_idx)
    if bracket_start < 0:
        raise ValueError(f'Could not find "[" for value "{value_name}"')

    depth = 0
    in_string = False
    escape = False
    for i in range(bracket_start, len(html)):
        ch = html[i]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
        elif ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                payload = html[bracket_start : i + 1]
                return json.loads(payload)

    raise ValueError(f'Could not find matching closing "]" for value "{value_name}"')


def _load_to_duckdb() -> None:
    db_path = Path(os.environ["DUCKDB_PATH"]).expanduser()
    directory_url = os.environ.get("DIRECTORY_URL", "https://directory.house.gov/")

    # Local import to keep DAG parse light, and because requests is present in Airflow images.
    import requests  # type: ignore

    db_path.parent.mkdir(parents=True, exist_ok=True)

    resp = requests.get(directory_url, timeout=60)
    resp.raise_for_status()
    html = resp.text

    employees = _extract_embedded_value_array(html, "employees")

    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        # DuckDB can read JSON from a file; this avoids requiring pandas/pyarrow.
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=True) as f:
            json.dump(employees, f)
            f.flush()
            con.execute(
                """
                CREATE OR REPLACE TABLE raw.employees_raw AS
                SELECT
                    CAST(name AS VARCHAR) AS name,
                    CAST(jobTitle AS VARCHAR) AS title,
                    CAST(worksFor.name AS VARCHAR) AS office,
                    CAST(address.streetAddress AS VARCHAR) AS address,
                    CAST(telephone AS VARCHAR) AS phone
                FROM read_json_auto(?)
                """,
                [f.name],
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
        task_id="load_to_duckdb",
        python_callable=_load_to_duckdb,
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
