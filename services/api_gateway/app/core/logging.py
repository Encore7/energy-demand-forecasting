"""Service logging wrapper (delegates to libs/observability)."""

from __future__ import annotations

from typing import Optional

from libs.observability.logging import configure_logging


def init_logging(
    *, service_name: str, service_version: Optional[str], log_level: Optional[str]
) -> None:
    """Initialize service logging."""
    configure_logging(
        service_name=service_name, service_version=service_version, log_level=log_level
    )
