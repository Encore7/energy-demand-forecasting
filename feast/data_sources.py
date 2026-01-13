from __future__ import annotations

from feast import FileSource

# We point to the PARQUET gold features stored in LakeFS via S3 gateway.
# Paths are "s3://<repo>/<branch>/...".
day_ahead_gold_source = FileSource(
    name="day_ahead_gold_features",
    path="s3://energy/main/gold/day_ahead_features/granularity=hour/",
    timestamp_field="event_ts_utc",
)
