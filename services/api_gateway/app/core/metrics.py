"""
Gateway metrics (OpenTelemetry).

Golden signals we emit:
- gateway_requests_total{downstream,method,status}
- gateway_request_duration_ms{downstream,method,status}
- gateway_upstream_errors_total{downstream,error_type}
- gateway_auth_denied_total{reason}
- gateway_rate_limited_total
"""

from __future__ import annotations

from typing import Mapping

from opentelemetry.metrics import Counter, Histogram

from libs.observability.metrics import get_meter

_meter = get_meter("api_gateway")

# Counters
gateway_requests_total: Counter[int] = _meter.create_counter(
    name="gateway_requests_total",
    description="Total number of gateway proxied requests",
    unit="1",
)

gateway_upstream_errors_total: Counter[int] = _meter.create_counter(
    name="gateway_upstream_errors_total",
    description="Total number of upstream errors encountered by the gateway",
    unit="1",
)

gateway_auth_denied_total: Counter[int] = _meter.create_counter(
    name="gateway_auth_denied_total",
    description="Total number of requests denied by gateway auth",
    unit="1",
)

gateway_rate_limited_total: Counter[int] = _meter.create_counter(
    name="gateway_rate_limited_total",
    description="Total number of requests blocked by gateway rate limiting",
    unit="1",
)

# Histograms
gateway_request_duration_ms: Histogram[float] = _meter.create_histogram(
    name="gateway_request_duration_ms",
    description="Gateway request latency (ms) for proxied requests",
    unit="ms",
)


def inc_auth_denied(reason: str) -> None:
    """Increment auth denied counter."""
    gateway_auth_denied_total.add(1, attributes={"reason": reason})


def inc_rate_limited() -> None:
    """Increment rate limited counter."""
    gateway_rate_limited_total.add(1)


def inc_upstream_error(downstream: str, error_type: str) -> None:
    """Increment upstream error counter."""
    gateway_upstream_errors_total.add(
        1, attributes={"downstream": downstream, "error_type": error_type}
    )


def observe_proxy_request(
    *,
    downstream: str,
    method: str,
    status_code: int,
    duration_ms: float,
) -> None:
    """Record request count + latency for a proxied request."""
    attrs: Mapping[str, str] = {
        "downstream": downstream,
        "method": method,
        "status": str(status_code),
    }
    gateway_requests_total.add(1, attributes=attrs)
    gateway_request_duration_ms.record(duration_ms, attributes=attrs)
