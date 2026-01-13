from __future__ import annotations

from feast import Entity

series = Entity(
    name="series_id",
    join_keys=["series_id"],
    description="Load series identifier (e.g. DE for national German load).",
)
