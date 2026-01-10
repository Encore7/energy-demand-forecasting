from __future__ import annotations

import io
import json
import os
from typing import Any, Dict, List

import boto3
import pyarrow as pa
import pyarrow.parquet as pq


def main() -> None:
    run_date = os.environ["RUN_DATE"]
    repo = os.environ["LAKEFS_REPO"]
    branch = os.environ["LAKEFS_BRANCH"]

    lakefs_s3_endpoint = os.environ["LAKEFS_S3_ENDPOINT_INTERNAL"]
    aws_key = os.environ["LAKEFS_ACCESS_KEY_ID"]
    aws_secret = os.environ["LAKEFS_SECRET_ACCESS_KEY"]
    region = os.environ["S3_REGION"]

    s3 = boto3.client(
        "s3",
        endpoint_url=lakefs_s3_endpoint,
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
        region_name=region,
    )

    bronze_prefix = (
        f"{branch}/bronze/calendar/source=derived/" f"ingestion_date={run_date}/"
    )

    # Find the bronze JSON object for this date
    resp = s3.list_objects_v2(Bucket=repo, Prefix=bronze_prefix)
    contents = resp.get("Contents", [])
    if not contents:
        raise RuntimeError(
            f"No bronze calendar objects found at prefix={bronze_prefix}"
        )

    # Single object per run_date
    key = sorted(o["Key"] for o in contents)[0]
    obj = s3.get_object(Bucket=repo, Key=key)
    payload = json.loads(obj["Body"].read().decode("utf-8"))

    rows: List[Dict[str, Any]] = payload.get("data", [])
    if len(rows) != 1:
        print(
            f"[WARN] Calendar bronze rows={len(rows)} for dt={run_date}, "
            "expected 1; continuing."
        )

    # Convert to Arrow Table
    table = pa.Table.from_pylist(rows)

    silver_prefix = f"{branch}/silver/calendar/dt={run_date}/"
    silver_key = f"{silver_prefix}calendar_{run_date}.parquet"

    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)

    s3.put_object(
        Bucket=repo,
        Key=silver_key,
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )

    print(f"Silver calendar written to: s3://{repo}/{silver_key}")


if __name__ == "__main__":
    main()
