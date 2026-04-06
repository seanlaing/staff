# staff — Employees CSV → DuckDB → dbt

End-to-end pattern: **Apache Airflow 3** loads `employees.csv` into a **local DuckDB** file, then **dbt** cleans and dedupes into `STAGING` and `MART` schemas inside that same database.

## Repository layout

| Path | Purpose |
|------|---------|
| `docker-compose.yaml` | Airflow 3.1.x (CeleryExecutor) + custom image |
| `docker/airflow/` | Image extending `apache/airflow:3.1.8` with `duckdb`, `dbt-duckdb`, `standard` provider |
| `dags/employees_etl.py` | Two DAGs: load CSV (producer **Asset**) → **asset-triggered** `dbt run` |
| `dbt/` | dbt: `stg_employees` → `dim_employees` + mart aggregates for dashboards |
| `data/` | `incoming/employees.csv` input; `staff.duckdb` created at runtime (gitignored) |

## Prerequisites

- Docker Desktop (or compatible engine) with enough RAM/CPU for Airflow ([Airflow docs](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#before-you-begin)).
- Your CSV at `data/incoming/employees.csv` (or override `EMPLOYEES_LOCAL_PATH` in `.env`).

## Run locally with Docker (step by step)

1. **Install** [Docker Desktop](https://www.docker.com/products/docker-desktop/) (Mac/Windows) or Docker Engine + Compose (Linux). Give Docker **at least ~4 GB RAM** in settings if things feel slow.

2. **From the repo root** (`staff/`):

   ```bash
   cp .env.example .env
   ```

   - **Linux only:** set `AIRFLOW_UID=$(id -u)` in `.env` so files in `./logs` and `./data` are not owned by root.
   - **macOS:** leaving `AIRFLOW_UID=50000` as in `.env.example` is usually fine.

3. **Put the CSV** where the DAG expects it:

   ```bash
   mkdir -p data/incoming
   cp /path/to/employees.csv data/incoming/employees.csv
   ```

4. **Build and start** Airflow (first run builds the custom image; it can take several minutes):

   ```bash
   docker compose build
   docker compose up -d
   ```

5. **Open the UI:** [http://localhost:8080](http://localhost:8080) (or `http://localhost:$AIRFLOW_WEB_PORT` if you changed it in `.env`).  
   Log in with `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD` from `.env` (defaults: `airflow` / `airflow`).

   If Compose reports **“port is already allocated”** for `8080`, something else is using that port (another Airflow stack, a dev server, etc.). Set `AIRFLOW_WEB_PORT=8081` in `.env`, run `docker compose up -d` again, and open that port instead.

6. **Run the pipeline**
   - Turn **On** both DAGs: **`employees_load_duckdb`** and **`employees_dbt_transform`**.
   - **Trigger** **`employees_load_duckdb`** (manual or your own schedule). When **`load_csv_to_duckdb`** succeeds, Airflow updates the logical asset `staff://duckdb/raw/employees_raw` and **automatically schedules** a run of **`employees_dbt_transform`** (no separate trigger needed for dbt).

7. **Stop** when done:

   ```bash
   docker compose down
   ```

   Your DuckDB file and CSV stay on the host under `./data/` (`staff.duckdb` is gitignored).

## Local Airflow + dbt (Docker) — short version

```bash
cp .env.example .env
docker compose build && docker compose up -d
```

**Asset scheduling:** the load task declares `outlets=[Asset("staff://duckdb/raw/employees_raw")]`. The dbt DAG uses `schedule=[that same Asset]`, so it runs **after** each successful load. Check **Browse → Assets** in the UI to see the link between DAGs.

## dbt only on your machine (no Airflow)

Use the same DuckDB path the DAG would use on the host:

```bash
export DUCKDB_PATH="$PWD/data/staff.duckdb"
# Load raw once (or run the DAG once to create the file), then:
cd dbt
dbt run --profiles-dir .
```

If the database file does not exist yet, run the load step once (Airflow task or a short DuckDB script mirroring `dags/employees_etl.py`).

## dbt models

- **`stg_employees`** — trim fields, empty strings → null, phone digits only.
- **`dim_employees`** — drops rows with null/empty **name, title, office**, or **phone** (requires `length(phone_digits) >= 10`); dedupes on `(employee_name, phone_digits)`.
- **`employees_deduped`** — same as `dim_employees` (legacy name).
- **`mart_headcount_by_office`** — counts + distinct titles per office (charts / filters).
- **`mart_headcount_by_title`** — counts + office spread per title.
- **`mart_dashboard_kpis`** — single-row KPIs (totals, distinct dimensions, refresh time).

Point **Metabase, Lightdash, Hex, etc.** at the DuckDB file (or sync these tables elsewhere) and use the `mart_*` models for tiles and `dim_employees` for drill-through.

## CI

GitHub Actions (`.github/workflows/ci.yml`) runs `dbt parse` with `dbt-duckdb`, a DAG AST check, and `docker compose config`. No warehouse credentials are required.

## Optional: moving back to Snowflake later

Keep this repo as a local-dev pattern, or reintroduce a warehouse by swapping the load task for `COPY`/`PUT` (or an object-store ingest) and changing `dbt` to `dbt-snowflake` in `profiles.yml` and `docker/airflow/requirements.txt`.

---

**Note:** `data/*.duckdb` is gitignored. Back up the file if it becomes your source of truth.
