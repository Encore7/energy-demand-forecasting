from __future__ import annotations

import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pyarrow.dataset as ds
import s3fs
from sqlalchemy import create_engine


def psi(expected: np.ndarray, actual: np.ndarray, bins: int = 10) -> float:
    """Simple PSI implementation on 1D arrays."""
    eps = 1e-6
    quantiles = np.linspace(0, 1, bins + 1)
    cuts = np.quantile(expected, quantiles)

    expected_counts, _ = np.histogram(expected, bins=cuts)
    actual_counts, _ = np.histogram(actual, bins=cuts)

    expected_perc = expected_counts / (len(expected) + eps)
    actual_perc = actual_counts / (len(actual) + eps)

    psi_vals = (actual_perc - expected_perc) * np.log(
        (actual_perc + eps) / (expected_perc + eps)
    )
    return float(np.sum(psi_vals))


def main() -> None:
    # We'll evaluate drift on feature distributions as of RUN_DATE-1
    run_date_str = os.environ["RUN_DATE"]
    eval_date = datetime.fromisoformat(run_date_str).date()

    history_days = int(os.environ.get("FEATURE_HISTORY_DAYS", 35))
    recent_window = 7
    baseline_window = history_days - recent_window

    repo = os.environ["LAKEFS_REPO"]
    branch = os.environ["LAKEFS_BRANCH"]
    lakefs_s3_endpoint = os.environ["LAKEFS_S3_ENDPOINT_INTERNAL"]
    aws_key = os.environ["LAKEFS_ACCESS_KEY_ID"]
    aws_secret = os.environ["LAKEFS_SECRET_ACCESS_KEY"]
    region = os.environ["S3_REGION"]

    forecast_store_dsn = os.environ["FORECAST_STORE_DSN"]

    fs = s3fs.S3FileSystem(
        key=aws_key,
        secret=aws_secret,
        client_kwargs={"endpoint_url": lakefs_s3_endpoint, "region_name": region},
    )

    gold_uri = f"{repo}/{branch}/gold/day_ahead_features/granularity=hour/"
    dataset = ds.dataset(gold_uri, filesystem=fs, format="parquet")
    table = dataset.to_table()
    df = table.to_pandas()

    df["dt"] = pd.to_datetime(df["dt"]).dt.date

    recent_start = eval_date - timedelta(days=recent_window)
    baseline_start = eval_date - timedelta(days=history_days)
    baseline_end = recent_start - timedelta(days=1)

    recent = df[(df["dt"] >= recent_start) & (df["dt"] <= eval_date)]
    baseline = df[(df["dt"] >= baseline_start) & (df["dt"] <= baseline_end)]

    if recent.empty or baseline.empty:
        print("[drift] Not enough data for drift detection, skipping")
        return

    features_to_check = ["load_mw", "temperature_2m"]
    rows = []

    for feat in features_to_check:
        if feat not in df.columns:
            continue
        v_recent = recent[feat].dropna().to_numpy()
        v_base = baseline[feat].dropna().to_numpy()
        if len(v_recent) == 0 or len(v_base) == 0:
            continue
        psi_val = psi(v_base, v_recent, bins=10)
        rows.append({"feature": feat, "psi": psi_val})

    if not rows:
        print("[drift] No usable features for PSI, skipping")
        return

    drift_df = pd.DataFrame(rows)
    drift_df["eval_date"] = eval_date
    drift_df["created_at_utc"] = pd.Timestamp.utcnow()
    drift_df["is_drift"] = drift_df["psi"] > 0.2  # simple threshold

    engine = create_engine(forecast_store_dsn)
    drift_df.to_sql(
        "day_ahead_drift",
        engine,
        if_exists="append",
        index=False,
    )

    print("[drift] PSI results:\n", drift_df)


if __name__ == "__main__":
    main()
