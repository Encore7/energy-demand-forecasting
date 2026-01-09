from __future__ import annotations

import json
import os
from datetime import date, datetime, timezone
from typing import Any, Dict, List

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

    # Reuse weather location config so calendar aligns with it
    location_name = os.environ.get("WEATHER_LOCATION_NAME", "berlin")
    lat = float(os.environ.get("WEATHER_LAT", "52.5200"))
    lon = float(os.environ.get("WEATHER_LON", "13.4050"))

    # Infra config
    lakefs_s3_endpoint = os.environ["LAKEFS_S3_ENDPOINT_INTERNAL"]
    aws_key = os.environ["LAKEFS_ACCESS_KEY_ID"]
    aws_secret = os.environ["LAKEFS_SECRET_ACCESS_KEY"]
    region = os.environ["S3_REGION"]

    repo = os.environ["LAKEFS_REPO"]
    branch = os.environ["LAKEFS_BRANCH"]

    rows: List[Dict[str, Any]] = []
    # Simple: weekends only for now; holidays can be added later
    weekday = run_date.weekday()  # 0=Mon, 6=Sun
    is_weekend = weekday >= 5
    season = _season_for_month(run_date.month)

    for hour in range(24):
        ts = datetime(
            run_date.year,
            run_date.month,
            run_date.day,
            hour,
            0,
            0,
            tzinfo=timezone.utc,
        )

        row = {
            "event_ts_utc": ts.isoformat(),
            "dt": run_date_str,
            "hour": hour,
            "day_of_week": weekday,  # 0=Mon..6=Sun
            "day_of_month": run_date.day,
            "week_of_year": run_date.isocalendar().week,
            "month": run_date.month,
            "year": run_date.year,
            "is_weekend": is_weekend,
            "is_holiday": False,  # placeholder for future Germany holidays
            "is_working_day": not is_weekend,
            "season": season,
            "location_name": location_name,
            "latitude": lat,
            "longitude": lon,
        }
        rows.append(row)

    payload: Dict[str, Any] = {
        "metadata": {
            "run_date": run_date_str,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "source": "derived-calendar",
            "location_name": location_name,
            "latitude": lat,
            "longitude": lon,
            "timezone": "UTC",
        },
        "data": rows,
    }

    s3 = boto3.client(
        "s3",
        endpoint_url=lakefs_s3_endpoint,
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
        region_name=region,
    )

    bronze_prefix = (
        f"{branch}/bronze/calendar/source=derived/"
        f"location={location_name}/"
        f"ingestion_date={run_date_str}/"
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
