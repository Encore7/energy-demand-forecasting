from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

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
    "MLFLOW_TRACKING_URI_INTERNAL": os.environ["MLFLOW_TRACKING_URI_INTERNAL"],
    "FORECAST_STORE_DSN": os.environ["FORECAST_STORE_DSN"],
}


def bash_env_cmd(module_or_path: str, is_module: bool) -> str:
    python_call = (
        f"python -m {module_or_path}" if is_module else f"python {module_or_path}"
    )
    common_env_inline = " ".join(f"{k}='{v}'" for k, v in COMMON_ENV.items())
    return (
        "set -euo pipefail; "
        "cd /opt/edf; "
        f"RUN_DATE='{{{{ ds }}}}' {common_env_inline} {python_call}"
    )


with DAG(
    dag_id="dayahead_batch_inference",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["dayahead", "inference", "batch"],
) as dag:

    # Dayahead_ingestion + dayahead_training already ran for this ds
    run_batch_forecast = BashOperator(
        task_id="run_day_ahead_batch_forecast",
        bash_command=bash_env_cmd(
            "pipelines/inference/dayahead_batch_forecast",
            is_module=True,
        ),
    )
