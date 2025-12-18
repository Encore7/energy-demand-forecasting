"""
Shared, typed configuration (12-factor) for all services.

Uses pydantic-settings so config is:
- typed
- validated
- env-driven
- consistent across services

Rule:
- libs/* should define *shared* settings building blocks
- each service defines its own Settings that composes these blocks
"""

from __future__ import annotations

from typing import Literal, Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

LogFormat = Literal["json", "text"]
OtlpProtocol = Literal["http/protobuf", "grpc"]


class ObservabilitySettings(BaseSettings):
    """Observability settings shared by all services."""

    model_config = SettingsConfigDict(env_prefix="", extra="ignore")

    log_level: str = Field(default="info", alias="LOG_LEVEL")
    log_format: LogFormat = Field(default="json", alias="LOG_FORMAT")

    # OTLP exporter configuration (service -> OTel Collector)
    otel_exporter_otlp_endpoint: str = Field(
        default="http://localhost:4318",
        alias="OTEL_EXPORTER_OTLP_ENDPOINT",
        description="Base OTLP endpoint. For local compose, use http://localhost:4318",
    )
    otel_exporter_otlp_protocol: OtlpProtocol = Field(
        default="http/protobuf",
        alias="OTEL_EXPORTER_OTLP_PROTOCOL",
        description="OTLP protocol (http/protobuf or grpc)",
    )

    otel_traces_exporter: str = Field(default="otlp", alias="OTEL_TRACES_EXPORTER")
    otel_metrics_exporter: str = Field(default="otlp", alias="OTEL_METRICS_EXPORTER")
    otel_logs_exporter: str = Field(default="none", alias="OTEL_LOGS_EXPORTER")

    metric_export_interval_seconds: int = Field(
        default=10,
        alias="OTEL_METRIC_EXPORT_INTERVAL",
        ge=1,
        description="Metric export interval in seconds",
    )

    service_namespace: str = Field(
        default="energy-demand-forecasting",
        alias="OTEL_SERVICE_NAMESPACE",
        description="Logical namespace to group services",
    )


class HttpServerSettings(BaseSettings):
    """HTTP server settings shared by FastAPI services."""

    model_config = SettingsConfigDict(env_prefix="", extra="ignore")

    host: str = Field(default="0.0.0.0", alias="HOST")
    port: int = Field(default=8000, alias="PORT", ge=1, le=65535)

    # Paths used by infra/health checks, k8s readiness, etc.
    health_path: str = Field(default="/health", alias="HEALTH_PATH")
    ready_path: str = Field(default="/ready", alias="READY_PATH")


class ServiceIdentitySettings(BaseSettings):
    """Service identity used for telemetry resource attributes and logs."""

    model_config = SettingsConfigDict(env_prefix="", extra="ignore")

    service_name: str = Field(default="unknown-service", alias="SERVICE_NAME")
    service_version: Optional[str] = Field(default=None, alias="SERVICE_VERSION")
    environment: str = Field(default="local", alias="ENVIRONMENT")
