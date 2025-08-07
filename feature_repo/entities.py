from feast import Entity

region = Entity(
    name="region",
    join_keys=["region"],
    description="Geographic region or control area",
)
