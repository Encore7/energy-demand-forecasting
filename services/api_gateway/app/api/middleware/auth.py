"""
API Key authentication middleware.

- Validates X-API-Key
- Bypasses configured paths (health/ready/docs)
- Stores authenticated principal on request.state (for rate limiting later)
"""

from __future__ import annotations

import logging
from typing import Iterable

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response

from services.api_gateway.app.core.exceptions import AppError
from services.api_gateway.app.core.metrics import inc_auth_denied

logger = logging.getLogger(__name__)


def _parse_api_keys(raw: object) -> set[str]:
    """
    Parse API keys from various env shapes.

    Supported:
    - "k1,k2,k3"
    - ["k1","k2"] (already parsed)
    - ("k1","k2")
    """
    if raw is None:
        return set()

    if isinstance(raw, (list, tuple, set)):
        return {str(x).strip() for x in raw if str(x).strip()}

    text = str(raw).strip()
    if not text:
        return set()

    # Comma-separated list
    return {part.strip() for part in text.split(",") if part.strip()}


class ApiKeyAuthMiddleware(BaseHTTPMiddleware):
    """API key auth middleware."""

    def __init__(
        self,
        app: object,
        *,
        enabled: bool,
        header_name: str,
        api_keys_raw: object,
        bypass_paths: Iterable[str],
    ) -> None:
        super().__init__(app)
        self._enabled = enabled
        self._header_name = header_name
        self._api_keys = _parse_api_keys(api_keys_raw)
        self._bypass_paths = set(bypass_paths)

        if self._enabled and not self._api_keys:
            logger.warning(
                "API key auth enabled but no API keys configured",
                extra={"header": self._header_name},
            )

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        if not self._enabled:
            return await call_next(request)

        path = request.url.path
        if path in self._bypass_paths:
            return await call_next(request)

        api_key = request.headers.get(self._header_name)
        if not api_key:
            inc_auth_denied("missing_api_key")
            raise AppError(
                error_code="AUTH_MISSING",
                message=f"Missing {self._header_name} header",
                http_status=401,
            )

        if api_key not in self._api_keys:
            inc_auth_denied("invalid_api_key")
            raise AppError(
                error_code="AUTH_INVALID",
                message="Invalid API key",
                http_status=403,
            )

        # Save principal for future rate limiting / auditing
        request.state.api_key = api_key
        return await call_next(request)
