from __future__ import annotations

import os
from datetime import date, timedelta
from typing import List

import mlflow
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import s3fs
from sqlalchemy import create_engine

LABEL_COL = "target_load_mw"


def _load_parquet_to_df(
    fs: s3fs.S3FileSystem, bucket: str, prefix: str
) -> pd.DataFrame:
    """
    Load all Parquet files under bucket/prefix into a single DataFrame.
    """
    paths = fs.glob(f"{bucket}/{prefix}*.parquet")
    if not paths:
        raise RuntimeError(f"No Parquet files found under s3://{bucket}/{prefix}")
    tables = [pq.read_table(fs.open(path, "rb")) for path in paths]
    return pq.concat_tables(tables).to_pandas()


def _select_serving_features(df: pd.DataFrame) -> List[str]:
    """
    Choose feature columns for inference.

    Here we keep it simple:
      - numeric columns
      - drop label + obvious meta columns
    """
    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    drop_cols = {"series_id", LABEL_COL}
    feature_cols = [c for c in numeric_cols if c not in drop_cols]
    return feature_cols


def _get_best_lgbm_model(mlflow_uri: str) -> tuple[str, str]:
    """
    Choose the best LightGBM day-ahead model from MLflow
    based on lowest RMSE.

    Returns:
        (run_id, model_uri)
    """
    mlflow.set_tracking_uri(mlflow_uri)
    client = mlflow.tracking.MlflowClient()

    exp = client.get_experiment_by_name("day_ahead_lgbm")
    if exp is None:
        raise RuntimeError("MLflow experiment 'day_ahead_lgbm' not found")

    # get runs ordered by rmse ascending
    runs = mlflow.search_runs(
        experiment_ids=[exp.experiment_id],
        order_by=["metrics.rmse ASC"],
        max_results=1,
    )
    if runs.empty:
        raise RuntimeError("No runs found in 'day_ahead_lgbm' experiment")

    best = runs.iloc[0]
    run_id = best["run_id"]
    model_uri = f"runs:/{run_id}/model"  # logged model with artifact_path 'model'
    return run_id, model_uri


def main() -> None:
    run_date_str = os.environ["RUN_DATE"]  # same as other DAGs: YYYY-MM-DD
    repo = os.environ["LAKEFS_REPO"]
    branch = os.environ["LAKEFS_BRANCH"]

    lakefs_s3_endpoint = os.environ["LAKEFS_S3_ENDPOINT_INTERNAL"]
    aws_key = os.environ["LAKEFS_ACCESS_KEY_ID"]
    aws_secret = os.environ["LAKEFS_SECRET_ACCESS_KEY"]
    region = os.environ["S3_REGION"]

    mlflow_uri_internal = os.environ["MLFLOW_TRACKING_URI_INTERNAL"]
    forecast_store_dsn = os.environ["FORECAST_STORE_DSN"]

    # The gold features we use for inference:
    # feature day dt = RUN_DATE, horizon = 24h ahead.
    gold_prefix = (
        f"{branch}/gold/day_ahead_features/granularity=hour/dt={run_date_str}/"
    )

    print(f"[inference] RUN_DATE={run_date_str}")
    print(f"[inference] Reading gold features from s3://{repo}/{gold_prefix}")

    fs = s3fs.S3FileSystem(
        key=aws_key,
        secret=aws_secret,
        client_kwargs={"endpoint_url": lakefs_s3_endpoint, "region_name": region},
    )

    df = _load_parquet_to_df(fs, repo, gold_prefix)

    if "event_ts_utc" not in df.columns or "dt" not in df.columns:
        raise RuntimeError("Gold features must contain event_ts_utc and dt columns")

    df["event_ts_utc"] = pd.to_datetime(df["event_ts_utc"])
    df["dt"] = pd.to_datetime(df["dt"]).dt.date

    if LABEL_COL in df.columns:
        print("[inference] Dropping label column from features:", LABEL_COL)

    feature_cols = _select_serving_features(df)
    print(f"[inference] Using {len(feature_cols)} feature columns for inference")

    X = df[feature_cols]

    # Choose best LightGBM model from MLflow
    best_run_id, model_uri = _get_best_lgbm_model(mlflow_uri_internal)
    print(
        f"[inference] Using LGBM model from MLflow run_id={best_run_id}, uri={model_uri}"
    )

    model = mlflow.pyfunc.load_model(model_uri)

    preds = model.predict(X)
    preds = np.asarray(preds).reshape(-1)

    # Build prediction DataFrame for storage
    run_date = date.fromisoformat(run_date_str)
    target_date = run_date + timedelta(days=1)
    forecast_ts_utc = pd.Timestamp.utcnow()

    pred_df = pd.DataFrame(
        {
            "forecast_run_date": run_date,  # when this forecast was generated (feature day)
            "target_date": target_date,  # date being forecast
            "event_ts_utc": df["event_ts_utc"],  # hourly timestamps
            "series_id": df.get("series_id", "DE"),
            "y_hat_mw": preds,
            "model_name": "day_ahead_lgbm",
            "model_run_id": best_run_id,
            "horizon_hours": 24,
            "created_at_utc": forecast_ts_utc,
        }
    )

    print(f"[inference] Writing {len(pred_df)} predictions to forecast store")

    engine = create_engine(forecast_store_dsn)
    # Pandas/SQLAlchemy create table on first use
    pred_df.to_sql(
        "day_ahead_forecasts",
        engine,
        if_exists="append",
        index=False,
    )

    print("[inference] Done.")


if __name__ == "__main__":
    main()
