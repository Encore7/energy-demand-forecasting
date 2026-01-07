from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LakeFSLocation:
    """
    Spark-friendly lakeFS URI pattern via S3 gateway:

      s3a://{repo}/{branch}/{path}

    Example:
      s3a://energy/main/bronze/demand/ingestion_date=2026-01-07/
    """

    repo: str
    branch: str

    def uri(self, path: str) -> str:
        path = path.lstrip("/")
        return f"s3a://{self.repo}/{self.branch}/{path}"


def bronze_demand_prefix(dt: str) -> str:
    # dt is YYYY-MM-DD
    return f"bronze/demand/ingestion_date={dt}/"


def silver_demand_prefix(dt: str) -> str:
    return f"silver/demand/dt={dt}/"
