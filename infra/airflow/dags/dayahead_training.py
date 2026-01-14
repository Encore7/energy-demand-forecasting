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
    "MLFLOW_TRACKING_URI": os.environ["MLFLOW_TRACKING_URI_INTERNAL"],
    "TRAIN_HISTORY_DAYS": os.environ.get("TRAIN_HISTORY_DAYS", "365"),
    "VAL_DAYS": os.environ.get("VAL_DAYS", "30"),
    "MLFLOW_EXPERIMENT_NAME": os.environ.get(
        "MLFLOW_EXPERIMENT_NAME", "day_ahead_lgbm"
    ),
    "MODEL_NAME": os.environ.get("MODEL_NAME", "day_ahead_lgbm"),
    # TFT-specific defaults
    "MLFLOW_EXPERIMENT_NAME_TFT": os.environ.get(
        "MLFLOW_EXPERIMENT_NAME_TFT", "day_ahead_tft"
    ),
    "MODEL_NAME_TFT": os.environ.get("MODEL_NAME_TFT", "day_ahead_tft"),
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
    dag_id="dayahead_training",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["dayahead", "training", "gold", "mlflow"],
) as dag:
    build_trainset = BashOperator(
        task_id="build_day_ahead_trainset",
        bash_command=bash_env_cmd(
            "pipelines.training.datasets.build_day_ahead_trainset",
            is_module=True,
        ),
    )

    train_lgbm = BashOperator(
        task_id="train_day_ahead_lgbm",
        bash_command=bash_env_cmd(
            "pipelines/training/models/lgbm/train_day_ahead.py",
            is_module=False,
        ),
    )

    train_tft = BashOperator(
        task_id="train_day_ahead_tft",
        bash_command=bash_env_cmd(
            "pipelines/training/models/tft/train_day_ahead.py",
            is_module=False,
        ),
    )

    build_trainset >> [train_lgbm, train_tft]
