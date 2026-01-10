from __future__ import annotations

import os
from typing import Sequence

import pyarrow.compute as pc
import pyarrow.dataset as ds
import s3fs

# pylint: disable=no-member

REQUIRED_COLS: Sequence[str] = (
    "event_ts_utc",
    "dt",
    "source",
    "filter_id",
    "region",
    "resolution",
    "load_mw",
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
    strict = os.getenv("STRICT_DEMAND_VALIDATION", "true").lower() == "true"

    fs = s3fs.S3FileSystem(
        key=aws_key,
        secret=aws_secret,
        client_kwargs={
            "endpoint_url": lakefs_s3_endpoint,
            "region_name": region,
        },
    )

    silver_path = (
        f"{repo}/{branch}/silver/demand/source=smard/granularity=hour/dt={run_date}/"
    )

    # HARD CHECK 0: can we even read the dataset?
    try:
        dataset = ds.dataset(silver_path, filesystem=fs, format="parquet")
    except FileNotFoundError as exc:
        raise RuntimeError(f"Silver demand path not found: {silver_path}") from exc

    # Will raise if required columns are missing -> HARD schema check
    table = dataset.to_table(columns=list(REQUIRED_COLS))
    rows = table.num_rows

    # HARD CHECKS

    # 1) event_ts_utc must not be null
    if table.column("event_ts_utc").null_count > 0:
        raise AssertionError("event_ts_utc has nulls (structural schema issue)")

    # 2) dt must equal run_date for all rows
    dt_col = table.column("dt")
    dt_str = pc.cast(dt_col, "string")
    bad_dt = pc.sum(pc.not_equal(dt_str, run_date)).as_py()
    if bad_dt != 0:
        raise AssertionError(f"Found {bad_dt} rows where dt != {run_date}")

    # 3) No duplicate timestamps (structural)
    ts_col = table.column("event_ts_utc")
    unique_count = pc.count_distinct(ts_col).as_py()
    if unique_count != rows:
        raise AssertionError(
            f"Duplicate timestamps detected: rows={rows}, distinct_ts={unique_count}"
        )

    # SOFT CHECKS
    issues = []

    # S1) Expected 24 hourly rows
    if rows != 24:
        issues.append(f"Expected 24 hourly rows, got {rows}")

    # S2) load_mw should not be null (quality, not schema)
    load_col = table.column("load_mw")
    null_loads = load_col.null_count
    if null_loads > 0:
        issues.append(f"load_mw has {null_loads} nulls (missing hours or source gaps)")

    # S3) basic sanity: load_mw must be >= 0 (quality anomaly)
    negative = pc.sum(pc.less(load_col, 0)).as_py()
    if negative > 0:
        issues.append(f"Found {negative} negative load_mw values")

    # REPORT / FAIL LOGIC

    if issues:
        msg = f"Quality issues for silver demand dt={run_date}: " + " | ".join(issues)
        if strict:
            raise AssertionError(msg)
        else:
            print("[WARN]", msg)
            print(
                f"NON-STRICT PASS: silver demand dt={run_date} rows={rows} "
                f"distinct_ts={unique_count}"
            )
    else:
        print(
            f"PASS: silver demand dt={run_date} rows={rows} distinct_ts={unique_count}"
        )


if __name__ == "__main__":
    main()
