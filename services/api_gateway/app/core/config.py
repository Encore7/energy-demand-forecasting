"""
API Gateway settings.

Composes shared settings and adds downstream service routing + gateway policies.
"""

from __future__ import annotations

from typing import Annotated, Sequence

from pydantic import Field, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict

from libs.core.config import (
    HttpServerSettings,
    ObservabilitySettings,
    ServiceIdentitySettings,
)


class DownstreamSettings(BaseSettings):
    """Downstream service base URLs (internal network addresses)."""

    model_config = SettingsConfigDict(env_prefix="API_GATEWAY_", extra="ignore")

    ingestion_url: Annotated[HttpUrl, Field(default="http://ingestion:8000")] = "http://ingestion:8000"  # type: ignore[assignment]
    feature_store_url: Annotated[HttpUrl, Field(default="http://feature_store:8000")] = "http://feature_store:8000"  # type: ignore[assignment]
    forecasting_batch_url: Annotated[HttpUrl, Field(default="http://forecasting_batch:8000")] = "http://forecasting_batch:8000"  # type: ignore[assignment]
    forecasting_realtime_url: Annotated[HttpUrl, Field(default="http://forecasting_realtime:8000")] = "http://forecasting_realtime:8000"  # type: ignore[assignment]
    training_url: Annotated[HttpUrl, Field(default="http://training:8000")] = "http://training:8000"  # type: ignore[assignment]
    drift_url: Annotated[HttpUrl, Field(default="http://drift:8000")] = "http://drift:8000"  # type: ignore[assignment]


class GatewayHttpClientSettings(BaseSettings):
    """HTTP client behavior for the gateway (timeouts, etc.)."""

    model_config = SettingsConfigDict(env_prefix="API_GATEWAY_", extra="ignore")

    request_timeout_seconds: Annotated[float, Field(default=5.0, ge=0.1, le=60.0)] = 5.0


class AuthSettings(BaseSettings):
    """
    API Key authentication settings.

    For local demo:
      - enabled=true
      - api_keys has one key (store in .env, do not commit)

    Auth mechanism:
      - Client sends: X-API-Key: <key>
    """

    model_config = SettingsConfigDict(env_prefix="API_GATEWAY_", extra="ignore")

    enabled: Annotated[bool, Field(default=True)] = True

    # Comma-separated list of allowed keys via env: API_GATEWAY_API_KEYS="k1,k2"
    api_keys: Annotated[
        Sequence[str], Field(default_factory=tuple, alias="API_KEYS")
    ] = ()

    header_name: Annotated[str, Field(default="X-API-Key")] = "X-API-Key"

    # Paths to bypass auth (health/ready/docs)
    bypass_paths: Annotated[
        Sequence[str],
        Field(default_factory=lambda: ("/health", "/ready", "/docs", "/openapi.json")),
    ] = (
        "/health",
        "/ready",
        "/docs",
        "/openapi.json",
    )


class RateLimitSettings(BaseSettings):
    """
    In-memory rate limiting settings (token bucket).

    Notes:
    - In-memory limiter is per-instance (works for local demo).
    - In production, use Redis or an API gateway like Envoy/Kong.

    Environment variables (with API_GATEWAY_ prefix):
      - RATE_LIMIT_ENABLED=true|false
      - RATE_LIMIT_RPM=60
      - RATE_LIMIT_BURST=30
    """

    model_config = SettingsConfigDict(env_prefix="API_GATEWAY_", extra="ignore")

    enabled: Annotated[bool, Field(default=True, alias="RATE_LIMIT_ENABLED")] = True
    requests_per_minute: Annotated[
        int, Field(default=60, ge=1, alias="RATE_LIMIT_RPM")
    ] = 60
    burst: Annotated[int, Field(default=30, ge=1, alias="RATE_LIMIT_BURST")] = 30


class Settings(BaseSettings):
    """Typed settings for the api_gateway service."""

    model_config = SettingsConfigDict(env_prefix="API_GATEWAY_", extra="ignore")

    identity: ServiceIdentitySettings = ServiceIdentitySettings(
        service_name="api_gateway"
    )
    http: HttpServerSettings = HttpServerSettings()
    obs: ObservabilitySettings = ObservabilitySettings()

    downstream: DownstreamSettings = DownstreamSettings()
    client: GatewayHttpClientSettings = GatewayHttpClientSettings()

    auth: AuthSettings = AuthSettings()

    rate_limit: RateLimitSettings = RateLimitSettings()


def load_settings() -> Settings:
    """Load and validate settings from environment."""
    return Settings()
