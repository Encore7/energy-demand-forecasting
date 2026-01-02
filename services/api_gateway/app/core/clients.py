"""
HTTP client factory for the gateway.

We use a single shared httpx AsyncClient for connection pooling.
OTel httpx instrumentation (enabled globally) will propagate trace headers.
"""

from __future__ import annotations

from dataclasses import dataclass

import httpx


@dataclass(frozen=True)
class HttpClientConfig:
    """Configuration for shared http client."""

    timeout_seconds: float


def create_httpx_client(cfg: HttpClientConfig) -> httpx.AsyncClient:
    """
    Create a shared AsyncClient with sane defaults.

    Args:
        cfg: HTTP client config

    Returns:
        httpx.AsyncClient
    """
    timeout = httpx.Timeout(timeout=cfg.timeout_seconds)
    limits = httpx.Limits(max_connections=100, max_keepalive_connections=20)
    return httpx.AsyncClient(timeout=timeout, limits=limits)
