"""
Local dashboard for staff DuckDB (dbt marts).

Prereqs:
  1. Run Airflow/dbt so data/staff.duckdb exists with MART.* tables, or
  2. cd dbt && dbt run --profiles-dir .  with DUCKDB_PATH pointing at the file.

Run from repo root:
  cd streamlit && pip install -r requirements.txt && streamlit run app.py

Or set DUCKDB_PATH to an absolute path to the .duckdb file.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = REPO_ROOT / "data" / "staff.duckdb"


def db_path() -> Path:
    raw = os.environ.get("DUCKDB_PATH", str(DEFAULT_DB))
    return Path(raw).expanduser()


@st.cache_resource
def get_connection(path_str: str):
    return duckdb.connect(path_str, read_only=True)


def main() -> None:
    st.set_page_config(page_title="Staff dashboard", layout="wide")
    st.title("Staff — DuckDB dashboard")

    path = db_path()
    if not path.is_file():
        st.error(
            f"DuckDB file not found: `{path}`\n\n"
            "Create it by running the Airflow DAGs (load + dbt) or:\n"
            f"`export DUCKDB_PATH={path}` then `cd dbt && dbt run --profiles-dir .`"
        )
        return

    con = get_connection(str(path.resolve()))

    st.caption(f"Database: `{path}` (read-only)")

    # DuckDB lowercases unquoted schema names; dbt uses +schema: MART → schema `mart`.
    try:
        kpis = con.sql("SELECT * FROM mart.mart_dashboard_kpis").df()
    except Exception as e:
        st.warning(f"Could not read KPIs (run `dbt run` first?): {e}")
        kpis = pd.DataFrame()

    if not kpis.empty:
        row = kpis.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Employees", f"{int(row['total_employees']):,}")
        c2.metric("Offices", f"{int(row['distinct_offices']):,}")
        c3.metric("Job titles", f"{int(row['distinct_job_titles']):,}")
        c4.metric("Refreshed", str(row["data_refreshed_at"])[:19])

    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("Headcount by office")
        try:
            by_office = con.sql(
                "SELECT office, employee_count FROM mart.mart_headcount_by_office ORDER BY employee_count DESC LIMIT 25"
            ).df()
            if by_office.empty:
                st.info("No rows in mart_headcount_by_office.")
            else:
                st.bar_chart(by_office.set_index("office"))
        except Exception as e:
            st.error(str(e))

    with col_b:
        st.subheader("Top job titles")
        try:
            by_title = con.sql(
                "SELECT job_title, employee_count FROM mart.mart_headcount_by_title ORDER BY employee_count DESC LIMIT 15"
            ).df()
            st.dataframe(by_title, use_container_width=True, hide_index=True)
        except Exception as e:
            st.error(str(e))

    st.subheader("Employee sample (dim)")
    try:
        sample = con.sql(
            "SELECT employee_name, job_title, office, phone_digits FROM mart.dim_employees LIMIT 200"
        ).df()
        st.dataframe(sample, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(str(e))


if __name__ == "__main__":
    main()
