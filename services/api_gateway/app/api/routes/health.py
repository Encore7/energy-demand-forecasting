"""
Liveness endpoint.

Use for:
- container liveness
- basic process-level check

Should NOT check downstream dependencies.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter
from pydantic import BaseModel, Field

router = APIRouter()


class HealthResponse(BaseModel):
    """Liveness response payload."""

    status: Annotated[str, Field(description="Service liveness status")] = "ok"
    ts_utc: Annotated[str, Field(description="UTC timestamp (ISO 8601)")]


@router.get("/health", response_model=HealthResponse, tags=["system"])
def health() -> HealthResponse:
    """Return liveness status."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return HealthResponse(ts_utc=ts)
