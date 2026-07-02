# Extends the official Airflow image with dbt and the ingestion dependencies,
# so the DAG can run ingestion tasks and `dbt build` with BashOperator instead
# of needing a separate dbt container or Cosmos-style integration. Simple and
# transparent — every task is a command you could also run by hand.
FROM apache/airflow:2.10.5-python3.12

# dbt-postgres pinned to a known-good major; requests for the API clients.
# psycopg2 is already present in the base image (Airflow uses it for Postgres).
RUN pip install --no-cache-dir \
    dbt-postgres==1.9.0 \
    requests==2.32.3
