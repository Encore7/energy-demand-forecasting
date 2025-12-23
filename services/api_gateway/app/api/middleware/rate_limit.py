"""
In-memory token bucket rate limiting middleware.

- Keyed by API key (request.state.api_key), or falls back to client IP.
- Uses token bucket: tokens refill continuously at rate RPM/60 per second.
- Allows bursts up to 'burst' tokens.
- Returns 429 when bucket empty.

This is suitable for local demo. For distributed production, replace with Redis.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Iterable

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response

from services.api_gateway.app.core.exceptions import AppError
from services.api_gateway.app.core.metrics import inc_rate_limited

logger = logging.getLogger(__name__)


@dataclass
class _Bucket:
    capacity: float
    tokens: float
    refill_per_sec: float
    last_refill_ts: float


class TokenBucketLimiter:
    """Async-safe token bucket limiter."""

    def __init__(self, *, rpm: int, burst: int) -> None:
        self._rpm = rpm
        self._burst = burst
        self._refill_per_sec = float(rpm) / 60.0
        self._buckets: dict[str, _Bucket] = {}
        self._lock = asyncio.Lock()

    def _now(self) -> float:
        return time.monotonic()

    def _get_or_create_bucket(self, key: str) -> _Bucket:
        now = self._now()
        bucket = self._buckets.get(key)
        if bucket is None:
            bucket = _Bucket(
                capacity=float(self._burst),
                tokens=float(self._burst),
                refill_per_sec=self._refill_per_sec,
                last_refill_ts=now,
            )
            self._buckets[key] = bucket
        return bucket

    def _refill(self, bucket: _Bucket) -> None:
        now = self._now()
        elapsed = max(0.0, now - bucket.last_refill_ts)
        if elapsed <= 0:
            return
        bucket.tokens = min(
            bucket.capacity, bucket.tokens + elapsed * bucket.refill_per_sec
        )
        bucket.last_refill_ts = now

    async def allow(self, key: str, *, cost: float = 1.0) -> tuple[bool, int]:
        """
        Attempt to consume tokens.

        Returns:
            (allowed, retry_after_seconds)
        """
        async with self._lock:
            bucket = self._get_or_create_bucket(key)
            self._refill(bucket)

            if bucket.tokens >= cost:
                bucket.tokens -= cost
                return True, 0

            # Compute retry-after when at least 1 token becomes available
            missing = cost - bucket.tokens
            seconds = (
                int(max(1.0, missing / bucket.refill_per_sec))
                if bucket.refill_per_sec > 0
                else 60
            )
            return False, seconds


def _client_identity(request: Request) -> str:
    """
    Determine identity for rate limiting:
    - Prefer authenticated api_key from auth middleware
    - Else fallback to client IP
    """
    api_key = getattr(request.state, "api_key", None)
    if isinstance(api_key, str) and api_key.strip():
        return f"api_key:{api_key}"

    client = request.client.host if request.client else "unknown"
    return f"ip:{client}"


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Rate limiting middleware."""

    def __init__(
        self,
        app: object,
        *,
        enabled: bool,
        rpm: int,
        burst: int,
        bypass_paths: Iterable[str],
    ) -> None:
        super().__init__(app)
        self._enabled = enabled
        self._bypass_paths = set(bypass_paths)
        self._limiter = TokenBucketLimiter(rpm=rpm, burst=burst)

        if enabled:
            logger.info("Rate limiting enabled", extra={"rpm": rpm, "burst": burst})

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        if not self._enabled:
            return await call_next(request)

        if request.url.path in self._bypass_paths:
            return await call_next(request)

        identity = _client_identity(request)
        allowed, retry_after = await self._limiter.allow(identity, cost=1.0)

        if not allowed:
            inc_rate_limited()
            raise AppError(
                error_code="RATE_LIMITED",
                message="Too many requests",
                http_status=429,
                details={"retry_after_seconds": retry_after},
            )

        return await call_next(request)
