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
    "LAKEFS_REPO": os.getenv("LAKEFS_REPO", "energy"),
    "LAKEFS_BRANCH": os.getenv("LAKEFS_BRANCH", "main"),
    "LAKEFS_S3_ENDPOINT_INTERNAL": os.getenv(
        "LAKEFS_S3_ENDPOINT_INTERNAL", "http://lakefs:8000"
    ),
    "MINIO_ROOT_USER": os.getenv("MINIO_ROOT_USER", "minio"),
    "MINIO_ROOT_PASSWORD": os.getenv("MINIO_ROOT_PASSWORD", "minio12345"),
    "S3_REGION": os.getenv("S3_REGION", "us-east-1"),
    "MLFLOW_TRACKING_URI": os.getenv(
        "MLFLOW_TRACKING_URI_INTERNAL", "http://mlflow:5000"
    ),
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
    dag_id="dayahead_ingest_and_silver",
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

    spark_env = {
        **COMMON_ENV,
        "RUN_DATE": "{{ ds }}",  # Airflow macro, templated per DAG run
    }

    # 2) BRONZE -> SILVER (Spark)
    bronze_to_silver_demand = SparkSubmitOperator(
        task_id="spark_bronze_to_silver_demand",
        application="/opt/edf/pipelines/transforms/spark/bronze_to_silver_demand.py",
        conn_id="spark_default",
        verbose=True,
        env_vars=spark_env,
    )

    bronze_to_silver_weather = SparkSubmitOperator(
        task_id="spark_bronze_to_silver_weather",
        application="/opt/edf/pipelines/transforms/spark/bronze_to_silver_weather.py",
        conn_id="spark_default",
        verbose=True,
        env_vars=spark_env,
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

    # DAG DEPENDENCIES
    ingest_demand_to_bronze >> bronze_to_silver_demand >> validate_silver_demand
    ingest_weather_to_bronze >> bronze_to_silver_weather >> validate_silver_weather
