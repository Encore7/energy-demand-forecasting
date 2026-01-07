from __future__ import annotations

import json
import os
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import boto3

from pipelines.ingestion.sources.smard_client import SmardClient, SmardSeriesKey


def _dt_utc_range(run_date: str) -> tuple[datetime, datetime]:
    # run_date is YYYY-MM-DD; SMARD timestamps are ms epoch UTC
    start = datetime.fromisoformat(run_date).replace(tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


def _to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _pick_chunk_timestamp(timestamps_ms: List[int], day_start_ms: int) -> int:
    # choose greatest ts <= day_start to ensure the returned series includes the day
    candidates = [t for t in timestamps_ms if t <= day_start_ms]
    if not candidates:
        # fallback: earliest available
        return min(timestamps_ms)
    return max(candidates)


def main() -> None:
    run_date = os.environ["RUN_DATE"]  # YYYY-MM-DD

    # lakeFS S3 gateway endpoint for S3 clients (inside docker network)
    lakefs_s3_endpoint = os.environ.get(
        "LAKEFS_S3_ENDPOINT_INTERNAL", "http://lakefs:8000"
    )

    aws_key = os.environ.get("MINIO_ROOT_USER", "minio")
    aws_secret = os.environ.get("MINIO_ROOT_PASSWORD", "minio12345")
    region = os.environ.get("S3_REGION", "us-east-1")

    repo = os.environ.get("LAKEFS_REPO", "energy")
    branch = os.environ.get("LAKEFS_BRANCH", "main")

    # SMARD base (public)
    smard = SmardClient(base_url="https://www.smard.de/app/")

    key = SmardSeriesKey(
        filter_id="410", region="DE", resolution="hour"
    )  # total load, Germany :contentReference[oaicite:6]{index=6}
    start_dt, end_dt = _dt_utc_range(run_date)
    start_ms, end_ms = _to_ms(start_dt), _to_ms(end_dt)

    timestamps = smard.list_timestamps_ms(key)
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
