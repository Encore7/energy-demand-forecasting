from __future__ import annotations

import os

import pandas as pd
import pyarrow.parquet as pq
import s3fs
from sqlalchemy import create_engine


def main() -> None:
    # We treat RUN_DATE as the date for which actuals are available
    # (i.e. dt == RUN_DATE in gold).
    run_date_str = os.environ["RUN_DATE"]  # YYYY-MM-DD

    repo = os.environ["LAKEFS_REPO"]
    branch = os.environ["LAKEFS_BRANCH"]
    lakefs_s3_endpoint = os.environ["LAKEFS_S3_ENDPOINT_INTERNAL"]
    aws_key = os.environ["LAKEFS_ACCESS_KEY_ID"]
    aws_secret = os.environ["LAKEFS_SECRET_ACCESS_KEY"]
    region = os.environ["S3_REGION"]

    forecast_store_dsn = os.environ["FORECAST_STORE_DSN"]

    gold_prefix = (
        f"{branch}/gold/day_ahead_features/granularity=hour/dt={run_date_str}/"
    )
    print(
        f"[actuals] RUN_DATE={run_date_str}, reading gold from s3://{repo}/{gold_prefix}"
    )

    fs = s3fs.S3FileSystem(
        key=aws_key,
        secret=aws_secret,
        client_kwargs={"endpoint_url": lakefs_s3_endpoint, "region_name": region},
    )

    paths = fs.glob(f"{repo}/{gold_prefix}*.parquet")
    if not paths:
        raise RuntimeError(f"No gold Parquet for dt={run_date_str}")

    tables = [pq.read_table(fs.open(p, "rb")) for p in paths]
    df = pq.concat_tables(tables).to_pandas()

    if "event_ts_utc" not in df.columns or "load_mw" not in df.columns:
        raise RuntimeError("Gold must contain event_ts_utc and load_mw columns")

    df["event_ts_utc"] = pd.to_datetime(df["event_ts_utc"])
    df["dt"] = pd.to_datetime(df["dt"]).dt.date
    df["series_id"] = df.get("series_id", "DE")
    df["created_at_utc"] = pd.Timestamp.utcnow()

    out_cols = ["event_ts_utc", "series_id", "load_mw", "dt", "created_at_utc"]
    df_out = df[out_cols].copy()

    engine = create_engine(forecast_store_dsn)
    df_out.to_sql(
        "day_ahead_actuals",
        engine,
        if_exists="append",
        index=False,
    )

    print(
        f"[actuals] Wrote {len(df_out)} rows to day_ahead_actuals for dt={run_date_str}"
    )


if __name__ == "__main__":
    main()
