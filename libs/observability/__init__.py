"""
Shared observability helpers.

This package provides:
- Structured logging with trace/span correlation
- OpenTelemetry tracing and metrics providers (OTLP -> Collector)
- FastAPI/http client instrumentation helpers

Design goals:
- Safe defaults (local-first)
- Environment-variable overrides (12-factor)
- Optional instrumentation (won't crash if a dependency is missing)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .http_instrumentation import instrument_fastapi, instrument_http_clients
from .logging import configure_logging
from .metrics import init_metrics, shutdown_metrics
from .tracing import init_tracing, shutdown_tracing


@dataclass(frozen=True)
class ObservabilityHandle:
    """Holds references to telemetry providers for lifecycle management."""

    service_name: str
    service_version: Optional[str]


def setup_observability(
    *,
    service_name: str,
    service_version: Optional[str] = None,
    log_level: Optional[str] = None,
) -> ObservabilityHandle:
    """
    Initialize logging + tracing + metrics.

    This is intended to be called once per process (at service startup).

    Args:
        service_name: Logical service name (e.g., "api_gateway").
        service_version: Optional version string.
        log_level: Optional log level override. If None, reads env.

    Returns:
        ObservabilityHandle: For lifecycle management.
    """
    configure_logging(
        service_name=service_name, service_version=service_version, log_level=log_level
    )
    init_tracing(service_name=service_name, service_version=service_version)
    init_metrics(service_name=service_name, service_version=service_version)
    return ObservabilityHandle(
        service_name=service_name, service_version=service_version
    )


def shutdown_observability() -> None:
    """
    Flush and shutdown telemetry providers (best-effort).

    Call on service shutdown (FastAPI lifespan shutdown).
    """
    shutdown_metrics()
    shutdown_tracing()
