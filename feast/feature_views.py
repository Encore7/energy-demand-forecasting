from __future__ import annotations

from datetime import timedelta

from feast import FeatureView, Field
from feast.types import Float32, Int32

from .data_sources import day_ahead_gold_source
from .entities import series

day_ahead_features_view = FeatureView(
    name="day_ahead_demand_features",
    entities=[series],
    ttl=timedelta(days=30),
    schema=[
        # Demand features
        Field(name="load_mw", dtype=Float32),
        Field(name="load_lag_1h", dtype=Float32),
        Field(name="load_lag_2h", dtype=Float32),
        Field(name="load_lag_3h", dtype=Float32),
        Field(name="load_lag_6h", dtype=Float32),
        Field(name="load_lag_12h", dtype=Float32),
        Field(name="load_lag_24h", dtype=Float32),
        Field(name="load_lag_48h", dtype=Float32),
        Field(name="load_lag_7d", dtype=Float32),
        Field(name="load_lag_14d", dtype=Float32),
        Field(name="load_mean_7d_same_hour", dtype=Float32),
        Field(name="load_mean_30d_same_hour", dtype=Float32),
        Field(name="load_std_30d_same_hour", dtype=Float32),
        # Weather features
        Field(name="temperature_2m", dtype=Float32),
        Field(name="temp_lag_24h", dtype=Float32),
        Field(name="temp_mean_7d", dtype=Float32),
        Field(name="heating_degree", dtype=Float32),
        Field(name="cooling_degree", dtype=Float32),
        # Calendar/time features
        Field(name="hour", dtype=Int32),
        Field(name="day_of_week", dtype=Int32),
        Field(name="month", dtype=Int32),
        # add more as needed
    ],
    source=day_ahead_gold_source,
    tags={"task": "day_ahead_forecast"},
)
