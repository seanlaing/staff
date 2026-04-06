# staff — Employees CSV → Snowflake → dbt

End-to-end pattern: **Apache Airflow 3** loads `employees.csv` into **Snowflake** (cloud warehouse), then **dbt** cleans and dedupes into staging and mart schemas.

> **What runs in the cloud vs locally**
>
> - **Snowflake** and **dbt** targets are cloud-side; you connect from Airflow wherever it runs.
> - This repo gives you **Docker Compose for local Airflow** (dev) and **GitHub Actions** (CI) that validate `dbt parse` and Compose syntax.
> - **Hosted Airflow** (Astronomer, Amazon MWAA, Google Cloud Composer, etc.) is supported by shipping the same `dags/` and building a similar image; see [Cloud deployment](#cloud-deployment).

## Repository layout

| Path | Purpose |
|------|---------|
| `docker-compose.yaml` | Airflow 3.1.x (CeleryExecutor) + custom image |
| `docker/airflow/` | Image extending `apache/airflow:3.1.8` with Snowflake provider + `dbt-snowflake` |
| `dags/employees_etl.py` | `PUT` + `COPY` to `RAW.EMPLOYEES_RAW`, then `dbt run` |
| `dbt/` | dbt project (`stg_employees`, `employees_deduped`) |
| `scripts/snowflake_bootstrap.sql` | One-time Snowflake objects (DB, schemas, stage, file format, raw table) |
| `data/incoming/` | Mount point for `employees.csv` |

## Prerequisites

- Docker Desktop (or compatible engine) with enough RAM/CPU for Airflow ([Airflow docs](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#before-you-begin)).
- A **Snowflake** account and user with rights to run the bootstrap script and load data.
- Your CSV at `data/incoming/employees.csv` (or set `EMPLOYEES_LOCAL_PATH` in `.env`).

## One-time Snowflake setup

1. In Snowflake, run `scripts/snowflake_bootstrap.sql` (adjust database/schema names if you change `.env`).
2. In the Airflow UI, add Connection **`snowflake_default`** (type Snowflake) with host/account, login, password, schema `RAW`, database `EMPLOYEES_ETL`, warehouse, etc., matching your account.

## Local Airflow + dbt

```bash
cp .env.example .env
# Edit .env with Snowflake env vars (used by dbt inside the container).

echo "$AIRFLOW_UID"  # on Linux, export AIRFLOW_UID=$(id -u) in .env

docker compose build
docker compose up -d
```

Open the UI at `http://localhost:8080` (default user/password from `.env.example` unless changed).

Unpause DAG **`employees_snowflake_dbt`** and trigger a run after:

1. `employees.csv` is present under `data/incoming/`.
2. Connection `snowflake_default` is valid.

The DAG runs:

1. **`put_and_copy_raw`** — `PUT` from the worker filesystem into `@EMPLOYEES_ETL.RAW.EMPLOYEES_STAGE`, then `COPY INTO RAW.EMPLOYEES_RAW`.
2. **`dbt_run`** — `dbt run` using `/opt/airflow/dbt` and `profiles.yml` (env-driven secrets).

## dbt models

- **`stg_employees`** — trims fields, normalizes phone to digits.
- **`employees_deduped`** — mart table, one row per `(employee_name, phone_digits)`.

Run dbt tests locally (optional):

```bash
cd dbt
export $(grep -v '^#' ../.env | xargs)   # or set Snowflake env manually
dbt run --profiles-dir .
```

## Cloud deployment

You cannot “finish” Snowflake or hosted Airflow from this repo alone—credentials and a target environment are required. Typical production choices:

1. **Snowflake** — Keep as the system of record; store service credentials in a secrets manager; narrow grants to `RAW` write + `STAGING`/`MART` DDL/DML for the transform role.
2. **Airflow** — Build and push `docker/airflow` to your registry; deploy to **Astronomer**, **MWAA** (custom image constraints apply), **Composer 3**, or **Kubernetes** using upstream Helm charts. Mount or sync `dags/` and `dbt/` consistently with what Compose mounts today.
3. **dbt** — Either keep **dbt Core** on Airflow workers (this repo) or invoke **dbt Cloud** jobs from Airflow via API operator instead of `BashOperator`.
4. **CI** — GitHub Actions workflow `.github/workflows/ci.yml` runs on every push/PR: `dbt parse`, DAG AST check, `docker compose config`.

## GitHub Actions (cloud CI)

On push to `main`/`master` or on pull requests, workflows validate the dbt project and Compose file without connecting to Snowflake. Add repository secrets only if you later extend the workflow to run `dbt run` against a dev warehouse.

---

**Security:** Never commit `.env` or Snowflake passwords. Use Airflow Connections and masked variables in production.
