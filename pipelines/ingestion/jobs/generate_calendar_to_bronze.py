from __future__ import annotations

import json
import os
from datetime import date, datetime, timezone
from typing import Any, Dict

import boto3


def _season_for_month(month: int) -> str:
    # Simple meteorological seasons for northern hemisphere
    if month in (12, 1, 2):
        return "winter"
    if month in (3, 4, 5):
        return "spring"
    if month in (6, 7, 8):
        return "summer"
    return "autumn"


def main() -> None:
    run_date_str = os.environ["RUN_DATE"]  # YYYY-MM-DD
    run_date = date.fromisoformat(run_date_str)

    # Infra config
    lakefs_s3_endpoint = os.environ["LAKEFS_S3_ENDPOINT_INTERNAL"]
    aws_key = os.environ["LAKEFS_ACCESS_KEY_ID"]
    aws_secret = os.environ["LAKEFS_SECRET_ACCESS_KEY"]
    region = os.environ["S3_REGION"]

    repo = os.environ["LAKEFS_REPO"]
    branch = os.environ["LAKEFS_BRANCH"]

    weekday = run_date.weekday()  # 0=Mon, 6=Sun
    is_weekend = weekday >= 5
    season = _season_for_month(run_date.month)

    row: Dict[str, Any] = {
        "dt": run_date_str,
        "day_of_week": weekday,
        "day_of_month": run_date.day,
        "week_of_year": run_date.isocalendar().week,
        "month": run_date.month,
        "year": run_date.year,
        "is_weekend": is_weekend,
        "is_holiday": False,
        "is_working_day": not is_weekend,
        "season": season,
    }

    payload: Dict[str, Any] = {
        "metadata": {
            "run_date": run_date_str,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "source": "derived-calendar",
            "timezone": "UTC",
            "scope": "germany",
        },
        "data": [row],
    }

    s3 = boto3.client(
        "s3",
        endpoint_url=lakefs_s3_endpoint,
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
        region_name=region,
    )

    bronze_prefix = (
        f"{branch}/bronze/calendar/source=derived/" f"ingestion_date={run_date_str}/"
    )
    obj_key = f"{bronze_prefix}calendar_{run_date_str}.json"

    s3.put_object(
        Bucket=repo,
        Key=obj_key,
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json",
    )

    print(f"Wrote bronze calendar: s3://{repo}/{obj_key}")


if __name__ == "__main__":
    main()
