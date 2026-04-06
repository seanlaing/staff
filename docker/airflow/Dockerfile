# Extend official Airflow 3 image with Snowflake provider + dbt (runs via BashOperator).
FROM apache/airflow:3.1.8

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
