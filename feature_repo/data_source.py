from datetime import timedelta

from feast import FileSource

energy_source = FileSource(
    path="data/energy_features.parquet",
    timestamp_field="timestamp",
    created_timestamp_column="created_at",
)
