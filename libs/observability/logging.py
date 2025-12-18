"""
Structured logging with trace/span correlation.

This module configures stdlib logging to emit JSON logs to stdout.
Promtail can scrape container stdout and send to Loki.

It also injects:
- trace_id / span_id (from current OpenTelemetry context)
- service.name / service.version
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import Any, MutableMapping, Optional

from opentelemetry import trace


@dataclass(frozen=True)
class LoggingConfig:
    """Runtime logging configuration."""

    service_name: str
    service_version: Optional[str]
    level: str
    fmt: str  # "json" or "text"


def _env(name: str, default: str) -> str:
    return os.getenv(name, default).strip()


def _resolve_level(level: str) -> int:
    try:
        return getattr(logging, level.upper())
    except AttributeError:
        return logging.INFO


def _current_trace_span_ids() -> tuple[Optional[str], Optional[str]]:
    span = trace.get_current_span()
    ctx = span.get_span_context()
    if not ctx or not ctx.is_valid:
        return (None, None)
    trace_id = f"{ctx.trace_id:032x}"
    span_id = f"{ctx.span_id:016x}"
    return (trace_id, span_id)


class TraceContextFilter(logging.Filter):
    """Inject trace/span + service metadata into every LogRecord."""

    def __init__(self, *, service_name: str, service_version: Optional[str]) -> None:
        super().__init__()
        self._service_name = service_name
        self._service_version = service_version

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        trace_id, span_id = _current_trace_span_ids()
        setattr(record, "trace_id", trace_id)
        setattr(record, "span_id", span_id)
        setattr(record, "service_name", self._service_name)
        setattr(record, "service_version", self._service_version)
        return True


class JsonFormatter(logging.Formatter):
    """JSON formatter suitable for Loki ingestion."""

    def format(self, record: logging.LogRecord) -> str:
        payload: MutableMapping[str, Any] = {
            "ts": int(time.time() * 1000),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "service_name": getattr(record, "service_name", None),
            "service_version": getattr(record, "service_version", None),
            "trace_id": getattr(record, "trace_id", None),
            "span_id": getattr(record, "span_id", None),
        }

        # Optional structured extras: logger.info("x", extra={"foo": "bar"})
        # Avoid dumping huge internal objects.
        extra_keys = {
            k: v
            for k, v in record.__dict__.items()
            if k
            not in {
                "name",
                "msg",
                "args",
                "levelname",
                "levelno",
                "pathname",
                "filename",
                "module",
                "exc_info",
                "exc_text",
                "stack_info",
                "lineno",
                "funcName",
                "created",
                "msecs",
                "relativeCreated",
                "thread",
                "threadName",
                "processName",
                "process",
                "message",
                "trace_id",
                "span_id",
                "service_name",
                "service_version",
            }
        }
        if extra_keys:
            payload["extra"] = extra_keys

        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=False)


def configure_logging(
    *,
    service_name: str,
    service_version: Optional[str] = None,
    log_level: Optional[str] = None,
    fmt: Optional[str] = None,
) -> LoggingConfig:
    """
    Configure process-wide logging.

    Environment variables (defaults shown):
      - LOG_LEVEL=info
      - LOG_FORMAT=json  (json|text)

    Args:
        service_name: Service name.
        service_version: Optional version.
        log_level: Override for LOG_LEVEL.
        fmt: Override for LOG_FORMAT.

    Returns:
        LoggingConfig
    """
    level_str = (log_level or _env("LOG_LEVEL", _env("OBS_LOG_LEVEL", "info"))).lower()
    fmt_str = (fmt or _env("LOG_FORMAT", "json")).lower()

    cfg = LoggingConfig(
        service_name=service_name,
        service_version=service_version,
        level=level_str,
        fmt=fmt_str,
    )

    root = logging.getLogger()
    root.setLevel(_resolve_level(level_str))

    # Clear handlers to avoid duplicate logs on reloads.
    for h in list(root.handlers):
        root.removeHandler(h)

    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setLevel(_resolve_level(level_str))

    # Filter injects trace/span into the record.
    handler.addFilter(
        TraceContextFilter(service_name=service_name, service_version=service_version)
    )

    if fmt_str == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s %(levelname)s %(name)s "
                "service=%(service_name)s trace_id=%(trace_id)s span_id=%(span_id)s - %(message)s"
            )
        )

    root.addHandler(handler)

    # Reduce noisy loggers commonly found in FastAPI stacks
    for noisy in ("uvicorn.access", "uvicorn.error", "httpx", "urllib3"):
        logging.getLogger(noisy).setLevel(max(root.level, logging.INFO))

    return cfg
