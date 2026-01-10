from __future__ import annotations

import os
from typing import Sequence

import pyarrow.compute as pc
import pyarrow.dataset as ds
import s3fs

# pylint: disable=no-member

# Structural columns we absolutely require in gold
REQUIRED_COLS: Sequence[str] = (
    "event_ts_utc",
    "dt",
    "series_id",
    "load_mw",
    "target_load_mw",
)

# "Critical" feature columns we *expect* to be present & non-null
# (if missing / null -> soft issues, escalated to hard if STRICT_GOLD_VALIDATION=true)
CRITICAL_FEATURE_COLS: Sequence[str] = (
    "load_lag_7d",
    "load_mean_30d_same_hour",
)


def main() -> None:
    run_date = os.environ["RUN_DATE"]
    repo = os.environ["LAKEFS_REPO"]
    branch = os.environ["LAKEFS_BRANCH"]

    lakefs_s3_endpoint = os.environ["LAKEFS_S3_ENDPOINT_INTERNAL"]
    aws_key = os.environ["LAKEFS_ACCESS_KEY_ID"]
    aws_secret = os.environ["LAKEFS_SECRET_ACCESS_KEY"]
    region = os.environ["S3_REGION"]

    # Soft vs hard behavior flag
    strict = os.getenv("STRICT_GOLD_VALIDATION", "true").lower() == "true"

    fs = s3fs.S3FileSystem(
        key=aws_key,
        secret=aws_secret,
        client_kwargs={
            "endpoint_url": lakefs_s3_endpoint,
            "region_name": region,
        },
    )

    gold_path = (
        f"{repo}/{branch}/gold/day_ahead_features/" f"granularity=hour/dt={run_date}/"
    )

    # HARD CHECK 0: can we even read the dataset?
    try:
        dataset = ds.dataset(gold_path, filesystem=fs, format="parquet")
    except FileNotFoundError as exc:
        raise RuntimeError(
            f"Gold day-ahead features path not found: {gold_path}"
        ) from exc

    # Load full table (we want to inspect all columns)
    table = dataset.to_table()
    rows = table.num_rows
    col_names = set(table.schema.names)

    # HARD CHECK 1: required columns exist
    missing_required = [c for c in REQUIRED_COLS if c not in col_names]
    if missing_required:
        raise AssertionError(
            f"Gold features missing required columns for dt={run_date}: {missing_required}"
        )

    # Convenience handles
    ts_col = table.column("event_ts_utc")
    dt_col = table.column("dt")
    load_col = table.column("load_mw")
    target_col = table.column("target_load_mw")
    series_col = table.column("series_id")

    # HARD CHECK 2: event_ts_utc must not be null
    if ts_col.null_count > 0:
        raise AssertionError(
            "event_ts_utc has nulls in gold features (structural issue)"
        )

    # HARD CHECK 3: dt must equal run_date for all rows
    dt_str = pc.cast(dt_col, "string")
    bad_dt = pc.sum(pc.not_equal(dt_str, run_date)).as_py()
    if bad_dt != 0:
        raise AssertionError(f"Found {bad_dt} rows in gold where dt != {run_date}")

    # HARD CHECK 4: no duplicate timestamps (for now assume one national series)
    unique_ts = pc.count_distinct(ts_col).as_py()
    if unique_ts != rows:
        raise AssertionError(
            f"Duplicate timestamps in gold features: rows={rows}, distinct_ts={unique_ts}"
        )

    # HARD CHECK 5: series_id must not be null
    if series_col.null_count > 0:
        raise AssertionError("series_id has nulls in gold features (structural issue)")

    # SOFT CHECKS
    issues = []

    # S1) Expected 24 hourly rows (1 day ahead, 24 hours)
    if rows != 24:
        issues.append(f"Expected 24 hourly gold rows, got {rows}")

    # S2) load_mw should not be null
    null_loads = load_col.null_count
    if null_loads > 0:
        issues.append(f"load_mw has {null_loads} nulls in gold features")

    # S3) target_load_mw should not be null
    null_targets = target_col.null_count
    if null_targets > 0:
        issues.append(f"target_load_mw has {null_targets} nulls in gold features")

    # S4) basic sanity: load_mw must be >= 0
    negative_loads = pc.sum(pc.less(load_col, 0)).as_py()
    if negative_loads > 0:
        issues.append(
            f"Found {negative_loads} negative load_mw values in gold features"
        )

    # S5) critical feature columns should exist and not be null
    for c in CRITICAL_FEATURE_COLS:
        if c not in col_names:
            issues.append(f"Critical feature column {c} is missing in gold features")
            continue

        col = table.column(c)
        if col.null_count > 0:
            issues.append(
                f"Critical feature column {c} has {col.null_count} null values in gold features"
            )

    # REPORT / FAIL LOGIC
    if issues:
        msg = (
            f"Quality issues for gold day-ahead features dt={run_date}: "
            + " | ".join(issues)
        )
        if strict:
            raise AssertionError(msg)
        else:
            print("[WARN]", msg)
            print(
                f"NON-STRICT PASS: gold dt={run_date} rows={rows} "
                f"distinct_ts={unique_ts}"
            )
    else:
        print(
            f"PASS: gold day-ahead features dt={run_date} rows={rows} "
            f"distinct_ts={unique_ts}"
        )


if __name__ == "__main__":
    main()
