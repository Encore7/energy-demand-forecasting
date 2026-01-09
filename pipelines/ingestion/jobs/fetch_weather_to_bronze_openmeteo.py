from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict

import boto3

from pipelines.ingestion.sources.open_meteo_client import (
    OpenMeteoClient,
    OpenMeteoRequest,
)


def main() -> None:
    run_date = os.environ["RUN_DATE"]  # YYYY-MM-DD

    # Default: Berlin
    lat = float(os.environ.get("WEATHER_LAT", "52.5200"))
    lon = float(os.environ.get("WEATHER_LON", "13.4050"))
    location_name = os.environ.get("WEATHER_LOCATION_NAME", "berlin")

    # Infra config
    lakefs_s3_endpoint = os.environ["LAKEFS_S3_ENDPOINT_INTERNAL"]
    aws_key = os.environ["LAKEFS_ACCESS_KEY_ID"]
    aws_secret = os.environ["LAKEFS_SECRET_ACCESS_KEY"]
    region = os.environ["S3_REGION"]

    repo = os.environ["LAKEFS_REPO"]
    branch = os.environ["LAKEFS_BRANCH"]

    client = OpenMeteoClient()
    req = OpenMeteoRequest(
        latitude=lat,
        longitude=lon,
        start_date=run_date,
        end_date=run_date,
        timezone="UTC",
    )
    forecast = client.fetch_forecast(req)

    payload: Dict[str, Any] = {
        "metadata": {
            "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
            "source": "open-meteo",
            "location_name": location_name,
            "latitude": lat,
            "longitude": lon,
            "run_date": run_date,
            "timezone": "UTC",
            "hourly_vars": list(req.hourly),
        },
        "raw": forecast,
    }

    s3 = boto3.client(
        "s3",
        endpoint_url=lakefs_s3_endpoint,
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
        region_name=region,
    )

    bronze_prefix = (
        f"{branch}/bronze/weather/source=open-meteo/"
        f"location={location_name}/"
        f"ingestion_date={run_date}/"
    )
    obj_key = f"{bronze_prefix}weather_{run_date}.json"

    s3.put_object(
        Bucket=repo,
        Key=obj_key,
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json",
    )

    print(f"Wrote bronze weather: s3://{repo}/{obj_key}")


if __name__ == "__main__":
    main()
