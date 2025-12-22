"""
Service-level observability bootstrap.

This bridges:
- typed Settings (service config)
- shared libs/observability implementation
- FastAPI app instrumentation
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from fastapi import FastAPI

from libs.observability import (
    ObservabilityHandle,
    instrument_fastapi,
    instrument_http_clients,
    setup_observability,
)


@dataclass(frozen=True)
class ServiceObservability:
    """Holds service observability state for lifecycle management."""

    handle: ObservabilityHandle


def init_service_observability(
    *,
    app: FastAPI,
    service_name: str,
    service_version: Optional[str],
    log_level: str,
) -> ServiceObservability:
    """
    Initialize observability for this service.

    Args:
        app: FastAPI application.
        service_name: Logical service name (e.g. "api_gateway").
        service_version: Optional service version.
        log_level: Log level string (e.g. "info").

    Returns:
        ServiceObservability: handle to shutdown providers cleanly.
    """
    handle = setup_observability(
        service_name=service_name, service_version=service_version, log_level=log_level
    )
    instrument_fastapi(app)
    instrument_http_clients(enable_requests=True, enable_httpx=True)
    return ServiceObservability(handle=handle)
