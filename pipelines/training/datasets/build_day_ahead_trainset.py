from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Tuple

import pandas as pd
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import s3fs


def _compute_date_range(run_date_str: str) -> Tuple[str, str, str]:
    run_date = datetime.strptime(run_date_str, "%Y-%m-%d").date()
    history_days = int(os.environ.get("TRAIN_HISTORY_DAYS", "365"))
    val_days = int(os.environ.get("VAL_DAYS", "30"))

    end_date = run_date - timedelta(days=1)  # last date we can use
    start_date = end_date - timedelta(days=history_days - 1)
    val_start = end_date - timedelta(days=val_days - 1)

    return (
        start_date.strftime("%Y-%m-%d"),
        val_start.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
    )


def main() -> None:
    run_date = os.environ["RUN_DATE"]
    repo = os.environ["LAKEFS_REPO"]
    branch = os.environ["LAKEFS_BRANCH"]

    lakefs_s3_endpoint = os.environ["LAKEFS_S3_ENDPOINT_INTERNAL"]
    aws_key = os.environ["LAKEFS_ACCESS_KEY_ID"]
    aws_secret = os.environ["LAKEFS_SECRET_ACCESS_KEY"]
    region = os.environ["S3_REGION"]

    train_start, val_start, train_end = _compute_date_range(run_date)
    print(
        f"[build_trainset] run_date={run_date} history= [{train_start} .. {train_end}], "
        f"val starts at {val_start}"
    )

    fs = s3fs.S3FileSystem(
        key=aws_key,
        secret=aws_secret,
        client_kwargs={"endpoint_url": lakefs_s3_endpoint, "region_name": region},
    )

    gold_prefix = f"{repo}/{branch}/gold/day_ahead_features/granularity=hour/"
    dataset = ds.dataset(
        gold_prefix,
        filesystem=fs,
        format="parquet",
        partitioning="hive",
    )

    # Filter by dt range at Arrow level
    dt_field = ds.field("dt")
    full_table = dataset.to_table(
        filter=(dt_field >= train_start) & (dt_field <= train_end)
    )

    if full_table.num_rows == 0:
        raise RuntimeError(
            f"No gold rows found in [{train_start} .. {train_end}] for training"
        )

    df = full_table.to_pandas()
    # Ensure dt is datetime.date
    df["dt"] = pd.to_datetime(df["dt"]).dt.date

    # Split train/val by date (validation is the last VAL_DAYS window)
    val_start_date = datetime.strptime(val_start, "%Y-%m-%d").date()
    train_df = df[df["dt"] < val_start_date].copy()
    val_df = df[df["dt"] >= val_start_date].copy()

    if train_df.empty or val_df.empty:
        raise RuntimeError(
            f"Train/val split failed: train_rows={len(train_df)}, val_rows={len(val_df)}"
        )

    print(
        f"[build_trainset] train_rows={len(train_df)}, "
        f"val_rows={len(val_df)}, unique_train_days={train_df['dt'].nunique()}, "
        f"unique_val_days={val_df['dt'].nunique()}"
    )

    # Write back to LakeFS as artifacts
    out_prefix = f"{branch}/artifacts/day_ahead/trainsets/run_date={run_date}/"

    train_uri = f"{out_prefix}train.parquet"
    val_uri = f"{out_prefix}val.parquet"

    # Use pyarrow to write
    train_table = ds.table(train_df)
    val_table = ds.table(val_df)

    with fs.open(f"{repo}/{train_uri}", "wb") as f:
        pq.write_table(train_table, f)

    with fs.open(f"{repo}/{val_uri}", "wb") as f:
        pq.write_table(val_table, f)

    print(f"[build_trainset] wrote train to s3://{repo}/{train_uri}")
    print(f"[build_trainset] wrote val   to s3://{repo}/{val_uri}")


if __name__ == "__main__":
    main()
