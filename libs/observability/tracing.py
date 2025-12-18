"""
OpenTelemetry tracing initialization (OTLP -> Collector).

Defaults:
- OTLP endpoint: http://localhost:4318  (OTLP/HTTP)
- Protocol: http/protobuf

Env overrides (standard OTEL):
- OTEL_EXPORTER_OTLP_ENDPOINT
- OTEL_EXPORTER_OTLP_TRACES_ENDPOINT
- OTEL_EXPORTER_OTLP_PROTOCOL
- OTEL_EXPORTER_OTLP_TRACES_PROTOCOL
"""

from __future__ import annotations

import os
import threading
from typing import Optional

from opentelemetry import trace
from opentelemetry.sdk.resources import SERVICE_NAME, SERVICE_VERSION, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

_LOCK = threading.Lock()
_INITIALIZED = False


def _env(name: str, default: str) -> str:
    return os.getenv(name, default).strip()


def _otlp_protocol_for_traces() -> str:
    return _env(
        "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
        _env("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf"),
    ).lower()


def _otlp_endpoint_for_traces() -> str:
    return _env(
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        _env("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318"),
    ).rstrip("/")


def init_tracing(*, service_name: str, service_version: Optional[str] = None) -> None:
    """
    Initialize OpenTelemetry tracing.

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

        provider = TracerProvider(resource=Resource.create(resource_attrs))

        protocol = _otlp_protocol_for_traces()
        endpoint = _otlp_endpoint_for_traces()

        try:
            if protocol in {"grpc", "otlp/grpc"}:
                from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
                    OTLPSpanExporter,  # type: ignore
                )

                exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
            else:
                # default: http/protobuf
                from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
                    OTLPSpanExporter,  # type: ignore
                )

                exporter = OTLPSpanExporter(endpoint=f"{endpoint}/v1/traces")
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(
                f"Failed to initialize OTLP trace exporter (protocol={protocol}, endpoint={endpoint})"
            ) from exc

        provider.add_span_processor(BatchSpanProcessor(exporter))
        trace.set_tracer_provider(provider)

        _INITIALIZED = True


def shutdown_tracing() -> None:
    """Flush and shutdown tracing provider (best-effort)."""
    provider = trace.get_tracer_provider()
    if hasattr(provider, "shutdown"):
        try:
            provider.shutdown()  # type: ignore[attr-defined]
        except Exception:
            # best-effort shutdown
            return
