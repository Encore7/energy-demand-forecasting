from __future__ import annotations

import os

import pyarrow.compute as pc
import pyarrow.dataset as ds
import s3fs

# pylint: disable=no-member

REQUIRED = (
    "event_ts_utc",
    "dt",
    "location_name",
    "temperature_2m",
    "relative_humidity_2m",
    "wind_speed_10m",
    "cloud_cover",
    "precipitation",
    "surface_pressure",
)


def main() -> None:
    run_date = os.environ["RUN_DATE"]
    repo = os.environ["LAKEFS_REPO"]
    branch = os.environ["LAKEFS_BRANCH"]

    lakefs_s3_endpoint = os.environ["LAKEFS_S3_ENDPOINT_INTERNAL"]
    aws_key = os.environ["LAKEFS_ACCESS_KEY_ID"]
    aws_secret = os.environ["LAKEFS_SECRET_ACCESS_KEY"]
    region = os.environ["S3_REGION"]

    strict = os.getenv("STRICT_WEATHER_VALIDATION", "false").lower() == "true"

    fs = s3fs.S3FileSystem(
        key=aws_key,
        secret=aws_secret,
        client_kwargs={
            "endpoint_url": lakefs_s3_endpoint,
            "region_name": region,
        },
    )

    silver_path = f"{repo}/{branch}/silver/weather/source=open-meteo/granularity=hour/dt={run_date}/"

    # HARD CHECK 0: dataset exists and is readable
    try:
        dataset = ds.dataset(silver_path, filesystem=fs, format="parquet")
    except FileNotFoundError as exc:
        raise RuntimeError(f"Silver weather path not found: {silver_path}") from exc

    # HARD: required columns must exist; this will raise if any missing
    table = dataset.to_table(columns=list(REQUIRED))
    rows = table.num_rows

    # HARD CHECKS (structural correctness)

    # H1) event_ts_utc must not be null
    if table.column("event_ts_utc").null_count > 0:
        raise AssertionError("event_ts_utc has nulls (structural issue)")

    # H2) dt must equal run_date for all rows
    dt_col = table.column("dt")
    dt_str = pc.cast(dt_col, "string")
    bad_dt = pc.sum(pc.not_equal(dt_str, run_date)).as_py()
    if bad_dt != 0:
        raise AssertionError(f"Found {bad_dt} rows where dt != {run_date}")

    # H3) No duplicate timestamps (1 row per timestamp)
    ts_col = table.column("event_ts_utc")
    distinct_ts = pc.count_distinct(ts_col).as_py()
    if distinct_ts != rows:
        raise AssertionError(
            f"Duplicate timestamps in weather data: rows={rows}, distinct_ts={distinct_ts}"
        )

    # SOFT CHECKS (data quality, not schema)
    issues = []

    # S1) Expected 24 hourly rows – but allow fewer in non-strict mode
    if rows != 24:
        issues.append(f"Expected 24 hourly rows, got {rows}")

    # S2) Nulls in weather variables – quality issues, but imputable in ML
    for col in REQUIRED[3:]:
        col_nulls = table.column(col).null_count
        if col_nulls > 0:
            issues.append(f"{col} has {col_nulls} nulls")

    # REPORT / FAIL LOGIC

    if issues:
        msg = f"Quality issues for silver weather dt={run_date}: " + " | ".join(issues)
        if strict:
            raise AssertionError(msg)
        else:
            print("[WARN]", msg)
            print(
                f"NON-STRICT PASS: silver weather dt={run_date} "
                f"rows={rows} distinct_ts={distinct_ts}"
            )
    else:
        print(
            f"PASS: silver weather dt={run_date} "
            f"rows={rows} distinct_ts={distinct_ts}"
        )


if __name__ == "__main__":
    main()
