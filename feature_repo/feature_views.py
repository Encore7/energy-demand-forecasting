from datetime import timedelta

from feast import FeatureView, Field
from feast.types import Float32, Int32

from feature_repo.data_source import energy_source
from feature_repo.entities import region

energy_fv = FeatureView(
    name="energy_features",
    entities=[region],
    ttl=timedelta(days=7),
    schema=[
        Field(name="demand_last_hour", dtype=Float32),
        Field(name="demand_rolling_mean_6h", dtype=Float32),
        Field(name="hour_of_day", dtype=Int32),
        Field(name="day_of_week", dtype=Int32),
    ],
    online=True,
    source=energy_source,
)
