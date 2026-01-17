from __future__ import annotations

import os
from datetime import date, datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from sqlalchemy import create_engine

DEFAULT_ARGS = {
    "owner": "edf",
    "retries": 1,
}

# Env needed by monitoring scripts + SQL access
COMMON_ENV = {
    "LAKEFS_REPO": os.environ["LAKEFS_REPO"],
    "LAKEFS_BRANCH": os.environ["LAKEFS_BRANCH"],
    "LAKEFS_S3_ENDPOINT_INTERNAL": os.environ["LAKEFS_S3_ENDPOINT_INTERNAL"],
    "LAKEFS_ACCESS_KEY_ID": os.environ["LAKEFS_ACCESS_KEY_ID"],
    "LAKEFS_SECRET_ACCESS_KEY": os.environ["LAKEFS_SECRET_ACCESS_KEY"],
    "S3_REGION": os.environ["S3_REGION"],
    "FORECAST_STORE_DSN": os.environ["FORECAST_STORE_DSN"],
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
    common_env_inline = " ".join(f"{k}='{v}'" for k, v in COMMON_ENV.items())

    return (
        "set -euo pipefail; "
        "cd /opt/edf; "
        f"RUN_DATE='{{{{ ds }}}}' {common_env_inline} {python_call}"
    )


# Simple policy thresholds for retraining
MAPE_THRESHOLD = 6.0  # %
PSI_THRESHOLD = 0.2  # PSI for drift
LOOKBACK_DAYS = 7  # window to evaluate metrics / drift


def _should_retrain(forecast_store_dsn: str, ds_str: str) -> bool:
    """
    Decide whether to trigger retraining based on:
      - MAPE over last LOOKBACK_DAYS
      - Drift PSI over last LOOKBACK_DAYS
    """
    engine = create_engine(forecast_store_dsn)

    today = date.fromisoformat(ds_str)
    start_date = today - timedelta(days=LOOKBACK_DAYS)

    # 1) Check metrics table
    try:
        metrics_df = pd.read_sql(
            """
            SELECT target_date, mape, mae
            FROM day_ahead_metrics
            WHERE target_date BETWEEN %(start)s AND %(end)s
            ORDER BY target_date DESC
            """,
            engine,
            params={"start": start_date, "end": today},
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[retrain-decider] Failed to query day_ahead_metrics: {exc}")
        metrics_df = pd.DataFrame()

    metrics_bad = False
    if not metrics_df.empty:
        max_mape = metrics_df["mape"].max()
        print(
            f"[retrain-decider] Max MAPE in last {LOOKBACK_DAYS} days: {max_mape:.2f}"
        )
        if max_mape > MAPE_THRESHOLD:
            metrics_bad = True
            print(
                f"[retrain-decider] MAPE {max_mape:.2f} > threshold {MAPE_THRESHOLD}, flagging retrain."
            )
    else:
        print(
            "[retrain-decider] No metrics data available for lookback window; not using metrics as trigger."
        )

    # 2) Check drift table
    try:
        drift_df = pd.read_sql(
            """
            SELECT eval_date, feature, psi, is_drift
            FROM day_ahead_drift
            WHERE eval_date BETWEEN %(start)s AND %(end)s
            """,
            engine,
            params={"start": start_date, "end": today},
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[retrain-decider] Failed to query day_ahead_drift: {exc}")
        drift_df = pd.DataFrame()

    drift_bad = False
    if not drift_df.empty:
        # Either explicit is_drift flag or PSI above threshold
        flagged = drift_df[
            (drift_df["is_drift"] == True) | (drift_df["psi"] > PSI_THRESHOLD)
        ]
        if not flagged.empty:
            drift_bad = True
            print(f"[retrain-decider] Drift detected on features:\n{flagged}")
        else:
            print("[retrain-decider] No drift above threshold in lookback window.")
    else:
        print(
            "[retrain-decider] No drift data available for lookback window; not using drift as trigger."
        )

    return metrics_bad or drift_bad


def decide_retraining(**context) -> str:
    """
    Branch task:
      - if conditions met -> trigger_retraining
      - else -> skip_retraining
    """
    ds_str = context["ds"]
    forecast_store_dsn = os.environ["FORECAST_STORE_DSN"]

    should = _should_retrain(forecast_store_dsn, ds_str)

    if should:
        print("[retrain-decider] Conditions met -> triggering retraining DAG.")
        return "trigger_retraining"
    print("[retrain-decider] Conditions NOT met -> skip retraining.")
    return "skip_retraining"


with DAG(
    dag_id="dayahead_retraining",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["dayahead", "monitoring", "retraining"],
) as dag:

    backfill_actuals = BashOperator(
        task_id="backfill_actuals_from_gold",
        bash_command=bash_env_cmd(
            "pipelines.monitoring.backfill_day_ahead_actuals",
            is_module=True,
        ),
    )

    compute_metrics = BashOperator(
        task_id="compute_day_ahead_metrics",
        bash_command=bash_env_cmd(
            "pipelines.monitoring.compute_day_ahead_metrics",
            is_module=True,
        ),
    )

    detect_drift = BashOperator(
        task_id="detect_day_ahead_drift",
        bash_command=bash_env_cmd(
            "pipelines.monitoring.detect_drift_day_ahead",
            is_module=True,
        ),
    )

    branch_decider = BranchPythonOperator(
        task_id="decide_retraining",
        python_callable=decide_retraining,
        provide_context=True,
    )

    trigger_retraining = TriggerDagRunOperator(
        task_id="trigger_retraining",
        trigger_dag_id="dayahead_training",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    skip_retraining = EmptyOperator(
        task_id="skip_retraining",
    )

    # Dependencies
    backfill_actuals >> compute_metrics >> detect_drift >> branch_decider
    branch_decider >> trigger_retraining
    branch_decider >> skip_retraining
