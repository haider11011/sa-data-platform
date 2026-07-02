"""Daily ELT pipeline: ingest both sources, then build and test the warehouse.

Every task is a BashOperator running a command you could paste into a shell —
deliberately transparent so the orchestration adds scheduling, retries and
observability without hiding what actually executes.

The dbt stages are split (deps -> seed -> snapshot -> run -> test) rather than
one `dbt build` so each stage is separately visible, retryable and timed in
the Airflow UI. dbt writes its artifacts to /tmp inside the container because
the project directory is a read-mostly bind mount from the host.

The sources publish annually/quarterly, so @daily is more frequent than the
data strictly needs — the schedule demonstrates the orchestration pattern, and
idempotent full-refresh ingestion makes over-running harmless.
"""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

INGEST_ENV = {
    # Warehouse connection comes through docker-compose (x-airflow-common);
    # PYTHONPATH makes the mounted /opt/airflow/ingestion package importable.
    "PYTHONPATH": "/opt/airflow",
}

DBT = "dbt"
DBT_ENV = {
    # Project/profile discovery via env vars (dbt's CLI only accepts these
    # flags after the subcommand; env vars sidestep the ordering entirely).
    "DBT_PROJECT_DIR": "/opt/airflow/dbt",
    "DBT_PROFILES_DIR": "/opt/airflow/dbt",
    "DBT_TARGET_PATH": "/tmp/dbt/target",
    "DBT_LOG_PATH": "/tmp/dbt/logs",
    "DBT_PACKAGES_INSTALL_PATH": "/tmp/dbt/dbt_packages",
}

with DAG(
    dag_id="sa_data_platform",
    description="Ingest Data.SA road crashes + ABS population, then dbt build the star schema",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="Australia/Adelaide"),
    catchup=False,  # each run fully refreshes the warehouse; backfills are meaningless
    default_args={
        "retries": 2,  # public APIs blip; two retries ride out most of it
        "retry_delay": timedelta(minutes=5),
    },
    tags=["elt", "dbt", "data-sa", "abs"],
    doc_md=__doc__,
) as dag:

    with TaskGroup(group_id="ingest") as ingest:
        ingest_data_sa = BashOperator(
            task_id="ingest_data_sa_road_crashes",
            bash_command="python -m ingestion.load_raw --source data_sa",
            env=INGEST_ENV,
            append_env=True,
        )
        ingest_abs = BashOperator(
            task_id="ingest_abs_population",
            bash_command="python -m ingestion.load_raw --source abs",
            env=INGEST_ENV,
            append_env=True,
        )
        # no dependency between them: different source systems, run in parallel

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"{DBT} deps",
        env=DBT_ENV,
        append_env=True,
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"{DBT} seed --full-refresh",
        env=DBT_ENV,
        append_env=True,
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"{DBT} snapshot",
        env=DBT_ENV,
        append_env=True,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT} run",
        env=DBT_ENV,
        append_env=True,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT} test",
        env=DBT_ENV,
        append_env=True,
    )

    ingest >> dbt_deps >> dbt_seed >> dbt_snapshot >> dbt_run >> dbt_test
