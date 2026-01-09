from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import boto3

from pipelines.ingestion.sources.smard_client import SmardClient, SmardSeriesKey


def _dt_utc_range(run_date: str) -> tuple[datetime, datetime]:
    """Compute UTC [start, end) interval for the given run_date (YYYY-MM-DD)."""
    start = datetime.fromisoformat(run_date).replace(tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


def _to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _pick_chunk_timestamp(timestamps_ms: List[int], day_start_ms: int) -> int:
    """
    Choose the greatest timestamp <= day_start_ms so that the chunk
    we fetch from SMARD definitely includes the requested day.
    """
    candidates = [t for t in timestamps_ms if t <= day_start_ms]
    if not candidates:
        # Fallback: use earliest available chunk
        return min(timestamps_ms)
    return max(candidates)


def main() -> None:
    # Airflow injects RUN_DATE via env: RUN_DATE='{{ ds }}'
    run_date = os.environ["RUN_DATE"]  # YYYY-MM-DD

    # lakeFS S3 gateway endpoint for S3 clients (inside docker network)
    lakefs_s3_endpoint = os.environ["LAKEFS_S3_ENDPOINT_INTERNAL"]

    aws_key = os.environ["LAKEFS_ACCESS_KEY_ID"]
    aws_secret = os.environ["LAKEFS_SECRET_ACCESS_KEY"]
    region = os.environ["S3_REGION"]

    repo = os.environ["LAKEFS_REPO"]
    branch = os.environ["LAKEFS_BRANCH"]

    # SMARD base (public)
    smard = SmardClient(base_url="https://www.smard.de/app/")

    # total load, Germany, hourly resolution
    key = SmardSeriesKey(filter_id="410", region="DE", resolution="hour")

    start_dt, end_dt = _dt_utc_range(run_date)
    start_ms, end_ms = _to_ms(start_dt), _to_ms(end_dt)

    timestamps = smard.list_timestamps_ms(key)
    if not timestamps:
        raise RuntimeError(
            f"No SMARD timestamps available for key={key} (filter=410, DE, hour)"
        )

    chunk_ts = _pick_chunk_timestamp(timestamps, start_ms)
    chunk = smard.fetch_timeseries_chunk(key, chunk_ts)
    points = smard.parse_points(chunk)

    # filter points for the day
    day_points = [
        {"ts_ms": ts, "load_mw": val} for ts, val in points if start_ms <= ts < end_ms
    ]

    payload: Dict[str, Any] = {
        "metadata": {
            "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
            "source": "smard",
            "filter_id": key.filter_id,
            "region": key.region,
            "resolution": key.resolution,
            "chunk_timestamp_ms": int(chunk_ts),
            "run_date": run_date,
        },
        "data": day_points,
        "raw": chunk,  # keep raw for audit/replay
    }

    s3 = boto3.client(
        "s3",
        endpoint_url=lakefs_s3_endpoint,
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
        region_name=region,
    )

    bronze_prefix = (
        f"{branch}/bronze/demand/source=smard/"
        f"filter=410/region=DE/res=hour/"
        f"ingestion_date={run_date}/"
    )
    obj_key = f"{bronze_prefix}demand_{run_date}.json"

    s3.put_object(
        Bucket=repo,
        Key=obj_key,
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json",
    )

    print(f"Wrote bronze: s3://{repo}/{obj_key} points={len(day_points)}")


if __name__ == "__main__":
    main()
