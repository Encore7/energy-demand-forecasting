from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple
from urllib.parse import urljoin

import requests


@dataclass(frozen=True)
class SmardSeriesKey:
    """
    SMARD series key.
    - filter_id: e.g. '410' = total load (Netzlast)
    - region: 'DE'
    - resolution: 'hour', 'quarterhour', 'day', ...
    """

    filter_id: str
    region: str
    resolution: str  # 'hour'


class SmardClient:
    """
    SMARD API pattern (no query string):
      index:     https://www.smard.de/app/chart_data/{filter}/{region}/index_{resolution}.json
      timeseries:https://www.smard.de/app/chart_data/{filter}/{region}/{filterCopy}_{regionCopy}_{resolution}_{timestamp}.json

    Pattern and available filters/regions are documented here. :contentReference[oaicite:4]{index=4}
    """

    def __init__(self, base_url: str = "https://www.smard.de/app/") -> None:
        self.base_url = base_url.rstrip("/") + "/"
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "energy-demand-forecasting/1.0 (contact: you@example.com)",
                "Accept": "application/json",
            }
        )

    def _get_json(self, path: str, timeout_s: int = 30, retries: int = 5) -> Any:
        url = urljoin(self.base_url, path.lstrip("/"))
        last_exc: Exception | None = None
        for i in range(retries):
            try:
                resp = self.session.get(url, timeout=timeout_s)
                resp.raise_for_status()
                return resp.json()
            except Exception as exc:
                last_exc = exc
                time.sleep(min(2**i, 10))
        raise RuntimeError(f"SMARD request failed for {url}") from last_exc

    def list_timestamps_ms(self, key: SmardSeriesKey) -> List[int]:
        # index_{resolution}.json
        path = f"chart_data/{key.filter_id}/{key.region}/index_{key.resolution}.json"
        data = self._get_json(path)
        # usually a JSON array of ms timestamps; be defensive:
        if isinstance(data, list):
            return [int(x) for x in data]
        if (
            isinstance(data, dict)
            and "timestamps" in data
            and isinstance(data["timestamps"], list)
        ):
            return [int(x) for x in data["timestamps"]]
        raise ValueError(f"Unexpected index response shape: {type(data)}")

    def fetch_timeseries_chunk(
        self, key: SmardSeriesKey, chunk_timestamp_ms: int
    ) -> Dict[str, Any]:
        # {filterCopy}_{regionCopy}_{resolution}_{timestamp}.json
        # filterCopy/regionCopy must match filter/region (SMARD quirk). :contentReference[oaicite:5]{index=5}
        path = (
            f"chart_data/{key.filter_id}/{key.region}/"
            f"{key.filter_id}_{key.region}_{key.resolution}_{int(chunk_timestamp_ms)}.json"
        )
        data = self._get_json(path)
        if not isinstance(data, dict):
            raise ValueError(f"Unexpected timeseries response shape: {type(data)}")
        return data

    @staticmethod
    def parse_points(chunk: Dict[str, Any]) -> List[Tuple[int, float | None]]:
        """
        SMARD typically returns something like:
          { ..., "series": [[ts_ms, value], ...], ... }
        but we stay defensive and try common keys.
        """
        for k in ("series", "data", "values"):
            v = chunk.get(k)
            if isinstance(v, list) and (len(v) == 0 or isinstance(v[0], list)):
                out: List[Tuple[int, float | None]] = []
                for pair in v:
                    if not isinstance(pair, list) or len(pair) < 2:
                        continue
                    ts = int(pair[0])
                    val = pair[1]
                    out.append((ts, float(val) if val is not None else None))
                return out
        raise ValueError(
            "Could not find points array in SMARD chunk (expected series/data/values)"
        )
