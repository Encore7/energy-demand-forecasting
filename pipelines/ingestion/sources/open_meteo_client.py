from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict
from urllib.parse import urlencode

import requests


@dataclass(frozen=True)
class OpenMeteoRequest:
    latitude: float
    longitude: float
    start_date: str  # YYYY-MM-DD
    end_date: str  # YYYY-MM-DD
    timezone: str = "UTC"
    hourly: tuple[str, ...] = (
        "temperature_2m",
        "relative_humidity_2m",
        "wind_speed_10m",
        "cloud_cover",
        "precipitation",
        "surface_pressure",
    )


class OpenMeteoClient:
    """
    Open-Meteo Forecast API:
      https://api.open-meteo.com/v1/forecast?... (no API key)
    """

    def __init__(
        self, base_url: str = "https://api.open-meteo.com/v1/forecast"
    ) -> None:
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "energy-demand-forecasting/1.0 (contact: you@example.com)",
                "Accept": "application/json",
            }
        )

    def fetch_forecast(
        self, req: OpenMeteoRequest, timeout_s: int = 30, retries: int = 5
    ) -> Dict[str, Any]:
        params = {
            "latitude": req.latitude,
            "longitude": req.longitude,
            "start_date": req.start_date,
            "end_date": req.end_date,
            "timezone": req.timezone,
            "hourly": ",".join(req.hourly),
        }
        url = f"{self.base_url}?{urlencode(params)}"

        last_exc: Exception | None = None
        for i in range(retries):
            try:
                r = self.session.get(url, timeout=timeout_s)
                r.raise_for_status()
                data = r.json()
                if not isinstance(data, dict):
                    raise ValueError("Open-Meteo response was not a JSON object")
                return data
            except Exception as exc:
                last_exc = exc
                time.sleep(min(2**i, 10))

        raise RuntimeError(f"Open-Meteo request failed: {url}") from last_exc
