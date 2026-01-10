from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DEFAULT_ARGS = {
    "owner": "edf",
    "retries": 1,
}
COMMON_ENV = {
    "LAKEFS_REPO": os.environ["LAKEFS_REPO"],
    "LAKEFS_BRANCH": os.environ["LAKEFS_BRANCH"],
    "LAKEFS_S3_ENDPOINT_INTERNAL": os.environ["LAKEFS_S3_ENDPOINT_INTERNAL"],
    "LAKEFS_ACCESS_KEY_ID": os.environ["LAKEFS_ACCESS_KEY_ID"],
    "LAKEFS_SECRET_ACCESS_KEY": os.environ["LAKEFS_SECRET_ACCESS_KEY"],
    "S3_REGION": os.environ["S3_REGION"],
    "MLFLOW_TRACKING_URI": os.environ["MLFLOW_TRACKING_URI_INTERNAL"],
    "STRICT_DEMAND_VALIDATION": os.environ.get("STRICT_DEMAND_VALIDATION", "true"),
    "STRICT_WEATHER_VALIDATION": os.environ.get("STRICT_WEATHER_VALIDATION", "true"),
}


def bash_env_cmd(module_or_path: str, is_module: bool) -> str:
    """
    Build a bash command that:
    - cd into repo
    - sets RUN_DATE={{ ds }}
    - exports COMMON_ENV vars
    - runs python module or file
    """
    python_call = (
        f"python -m {module_or_path}" if is_module else f"python {module_or_path}"
    )

    common_env_inline = " ".join(
        f"{key}='{value}'" for key, value in COMMON_ENV.items()
    )

    # NOTE: '{{ ds }}' must be double-braced inside f-string so Airflow can template it
    return (
        "set -euo pipefail; "
        "cd /opt/edf; "
        f"RUN_DATE='{{{{ ds }}}}' {common_env_inline} {python_call}"
    )


with DAG(
    dag_id="dayahead_ingestion",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["dayahead", "phase1", "bronze", "silver"],
) as dag:

    # 1) INGEST -> BRONZE (Python via BashOperator)
    ingest_demand_to_bronze = BashOperator(
        task_id="ingest_demand_to_bronze_smard",
        bash_command=bash_env_cmd(
            "pipelines.ingestion.jobs.fetch_demand_to_bronze_smard",
            is_module=True,
        ),
    )

    ingest_weather_to_bronze = BashOperator(
        task_id="ingest_weather_to_bronze_openmeteo",
        bash_command=bash_env_cmd(
            "pipelines.ingestion.jobs.fetch_weather_to_bronze_openmeteo",
            is_module=True,
        ),
    )

    ingest_calendar_to_bronze = BashOperator(
        task_id="ingest_calendar_to_bronze",
        bash_command=bash_env_cmd(
            "pipelines.ingestion.jobs.generate_calendar_to_bronze",
            is_module=True,
        ),
    )

    spark_env = {
        **COMMON_ENV,
        "RUN_DATE": "{{ ds }}",  # Airflow macro, templated per DAG run
    }

    # 2) BRONZE -> SILVER (Spark)
    bronze_to_silver_demand = SparkSubmitOperator(
        task_id="spark_bronze_to_silver_demand",
        application="/opt/edf/pipelines/transforms/spark/bronze_to_silver_demand.py",
        conn_id="spark_default",
        name="bronze_to_silver_demand_smard",
        verbose=True,
        env_vars=spark_env,
    )

    bronze_to_silver_weather = SparkSubmitOperator(
        task_id="spark_bronze_to_silver_weather",
        application="/opt/edf/pipelines/transforms/spark/bronze_to_silver_weather.py",
        conn_id="spark_default",
        name="bronze_to_silver_weather_openmeteo",
        verbose=True,
        env_vars=spark_env,
    )

    bronze_to_silver_calendar = BashOperator(
        task_id="bronze_to_silver_calendar",
        bash_command=bash_env_cmd(
            "pipelines/transforms/spark/bronze_to_silver_calendar.py",
            is_module=False,
        ),
    )

    # 3) VALIDATE SILVER (GE-style checks; Python via BashOperator)
    validate_silver_demand = BashOperator(
        task_id="validate_silver_demand",
        bash_command=bash_env_cmd(
            "pipelines/transforms/quality/validate_silver_demand.py",
            is_module=False,
        ),
    )

    validate_silver_weather = BashOperator(
        task_id="validate_silver_weather",
        bash_command=bash_env_cmd(
            "pipelines/transforms/quality/validate_silver_weather.py",
            is_module=False,
        ),
    )

    validate_silver_calendar = BashOperator(
        task_id="validate_silver_calendar",
        bash_command=bash_env_cmd(
            "pipelines/transforms/quality/validate_silver_calendar.py",
            is_module=False,
        ),
    )

    # SILVER -> GOLD (Spark)
    compute_day_ahead_features = SparkSubmitOperator(
        task_id="spark_silver_to_gold_day_ahead_features",
        application="/opt/edf/pipelines/transforms/spark/silver_to_gold_day_ahead_features.py",
        conn_id="spark_default",
        name="silver_to_gold_day_ahead_features",
        verbose=True,
        env_vars=spark_env,
    )

    # VALIDATE GOLD FEATURES
    validate_gold_features = BashOperator(
        task_id="validate_gold_day_ahead_features",
        bash_command=bash_env_cmd(
            "pipelines/transforms/quality/validate_gold_day_ahead_features.py",
            is_module=False,
        ),
    )

    # DAG DEPENDENCIES
    ingest_demand_to_bronze >> bronze_to_silver_demand >> validate_silver_demand
    ingest_weather_to_bronze >> bronze_to_silver_weather >> validate_silver_weather
    ingest_calendar_to_bronze >> bronze_to_silver_calendar >> validate_silver_calendar
    (
        [
            validate_silver_demand,
            validate_silver_weather,
            validate_silver_calendar,
        ]
        >> compute_day_ahead_features
        >> validate_gold_features
    )
