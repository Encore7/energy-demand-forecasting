"""
OpenTelemetry metrics initialization (OTLP -> Collector).

Defaults:
- OTLP endpoint: http://localhost:4318  (OTLP/HTTP)
- Protocol: http/protobuf
- Export interval: 10s

Env overrides:
- OTEL_EXPORTER_OTLP_ENDPOINT
- OTEL_EXPORTER_OTLP_METRICS_ENDPOINT
- OTEL_EXPORTER_OTLP_PROTOCOL
- OTEL_EXPORTER_OTLP_METRICS_PROTOCOL
- OTEL_METRIC_EXPORT_INTERVAL  (seconds)
"""

from __future__ import annotations

import os
import threading
from typing import Optional

from opentelemetry import metrics
from opentelemetry.metrics import Meter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import SERVICE_NAME, SERVICE_VERSION, Resource

_LOCK = threading.Lock()
_INITIALIZED = False


def _env(name: str, default: str) -> str:
    return os.getenv(name, default).strip()


def _otlp_protocol_for_metrics() -> str:
    return _env(
        "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL",
        _env("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf"),
    ).lower()


def _otlp_endpoint_for_metrics() -> str:
    return _env(
        "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
        _env("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318"),
    ).rstrip("/")


def _export_interval_ms() -> int:
    raw = _env("OTEL_METRIC_EXPORT_INTERVAL", "10")
    try:
        seconds = int(raw)
    except ValueError:
        seconds = 10
    return max(1, seconds) * 1000


def init_metrics(*, service_name: str, service_version: Optional[str] = None) -> None:
    """
    Initialize OpenTelemetry metrics provider.

    Args:
        service_name: e.g. "api_gateway"
        service_version: optional version string
    """
    global _INITIALIZED
    with _LOCK:
        if _INITIALIZED:
            return

        resource_attrs = {SERVICE_NAME: service_name}
        if service_version:
            resource_attrs[SERVICE_VERSION] = service_version

        protocol = _otlp_protocol_for_metrics()
        endpoint = _otlp_endpoint_for_metrics()

        try:
            if protocol in {"grpc", "otlp/grpc"}:
                from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
                    OTLPMetricExporter,  # type: ignore
                )

                exporter = OTLPMetricExporter(endpoint=endpoint, insecure=True)
            else:
                from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
                    OTLPMetricExporter,  # type: ignore
                )

                exporter = OTLPMetricExporter(endpoint=f"{endpoint}/v1/metrics")
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(
                f"Failed to initialize OTLP metric exporter (protocol={protocol}, endpoint={endpoint})"
            ) from exc

        reader = PeriodicExportingMetricReader(
            exporter, export_interval_millis=_export_interval_ms()
        )
        provider = MeterProvider(
            resource=Resource.create(resource_attrs), metric_readers=[reader]
        )
        metrics.set_meter_provider(provider)

        _INITIALIZED = True


def shutdown_metrics() -> None:
    """Flush and shutdown metrics provider (best-effort)."""
    provider = metrics.get_meter_provider()
    if hasattr(provider, "shutdown"):
        try:
            provider.shutdown()  # type: ignore[attr-defined]
        except Exception:
            return


def get_meter(name: str) -> Meter:
    """
    Return a Meter for instrument creation.

    Args:
        name: instrumentation scope name (e.g. "api_gateway")

    Returns:
        Meter
    """
    return metrics.get_meter(name)
