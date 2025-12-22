"""
Exception types and FastAPI exception handlers.

Goals:
- consistent error response shape
- safe messages for clients
- rich logs for operators
- explicit exception chaining
"""

from __future__ import annotations

import logging
from typing import Annotated, Any, Optional

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class ErrorResponse(BaseModel):
    """Standard API error response."""

    error_code: Annotated[str, Field(description="Stable error code for clients")]
    message: Annotated[str, Field(description="Human readable error message")]
    trace_id: Annotated[
        Optional[str], Field(description="Trace id for correlation")
    ] = None
    details: Annotated[
        Optional[dict[str, Any]], Field(description="Optional error details")
    ] = None


class AppError(Exception):
    """Base application error (domain/infra)."""

    def __init__(
        self,
        *,
        error_code: str,
        message: str,
        http_status: int = 400,
        details: Optional[dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.message = message
        self.http_status = http_status
        self.details = details


def _trace_id_from_request(request: Request) -> Optional[str]:
    # populated by our logging filter from OTel context; as a fallback return None
    # We also try to read it from request state if we later add middleware.
    return getattr(request.state, "trace_id", None)


def install_exception_handlers(app: FastAPI) -> None:
    """Register exception handlers on the app."""

    @app.exception_handler(AppError)
    async def handle_app_error(request: Request, exc: AppError) -> JSONResponse:
        trace_id = _trace_id_from_request(request)
        logger.warning(
            "AppError",
            extra={
                "error_code": exc.error_code,
                "trace_id": trace_id,
                "details": exc.details,
            },
        )
        headers: dict[str, str] = {}
        if (
            exc.error_code == "RATE_LIMITED"
            and exc.details
            and "retry_after_seconds" in exc.details
        ):
            headers["Retry-After"] = str(exc.details["retry_after_seconds"])

        return JSONResponse(
            status_code=exc.http_status,
            content=ErrorResponse(
                error_code=exc.error_code,
                message=exc.message,
                trace_id=trace_id,
                details=exc.details,
            ).model_dump(),
            headers=headers,
        )

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        trace_id = _trace_id_from_request(request)
        logger.info(
            "RequestValidationError",
            extra={"trace_id": trace_id, "errors": exc.errors()},
        )
        return JSONResponse(
            status_code=422,
            content=ErrorResponse(
                error_code="VALIDATION_ERROR",
                message="Request validation failed",
                trace_id=trace_id,
                details={"errors": exc.errors()},
            ).model_dump(),
        )

    @app.exception_handler(Exception)
    async def handle_unhandled(request: Request, exc: Exception) -> JSONResponse:
        trace_id = _trace_id_from_request(request)
        logger.exception("Unhandled exception", extra={"trace_id": trace_id})
        return JSONResponse(
            status_code=500,
            content=ErrorResponse(
                error_code="INTERNAL_ERROR",
                message="Internal server error",
                trace_id=trace_id,
            ).model_dump(),
        )
