"""
HTTP instrumentation helpers.

- FastAPI: instrument incoming requests
- requests/httpx: instrument outgoing calls and propagate trace headers

All imports are optional: if the relevant instrumentation dependency is not
installed, we log a warning and continue (no crash).
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def instrument_fastapi(app: Any) -> None:
    """
    Instrument a FastAPI app (incoming HTTP requests).

    Args:
        app: FastAPI instance
    """
    try:
        from opentelemetry.instrumentation.fastapi import (
            FastAPIInstrumentor,  # type: ignore
        )

        FastAPIInstrumentor.instrument_app(app)
        logger.info("FastAPI OpenTelemetry instrumentation enabled")
    except ImportError:
        logger.warning(
            "FastAPI instrumentation not installed (opentelemetry-instrumentation-fastapi)"
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("FastAPI instrumentation failed", extra={"error": str(exc)})


def instrument_http_clients(
    *, enable_requests: bool = True, enable_httpx: bool = True
) -> None:
    """
    Instrument common outgoing HTTP client libraries.

    Args:
        enable_requests: instrument requests
        enable_httpx: instrument httpx
    """
    if enable_requests:
        try:
            from opentelemetry.instrumentation.requests import (
                RequestsInstrumentor,  # type: ignore
            )

            RequestsInstrumentor().instrument()
            logger.info("requests OpenTelemetry instrumentation enabled")
        except ImportError:
            logger.warning(
                "requests instrumentation not installed (opentelemetry-instrumentation-requests)"
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "requests instrumentation failed", extra={"error": str(exc)}
            )

    if enable_httpx:
        try:
            from opentelemetry.instrumentation.httpx import (
                HTTPXClientInstrumentor,  # type: ignore
            )

            HTTPXClientInstrumentor().instrument()
            logger.info("httpx OpenTelemetry instrumentation enabled")
        except ImportError:
            logger.warning(
                "httpx instrumentation not installed (opentelemetry-instrumentation-httpx)"
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("httpx instrumentation failed", extra={"error": str(exc)})
