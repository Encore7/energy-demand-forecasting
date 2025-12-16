import logging
from platform.common.config import get_settings

from opentelemetry.trace import get_current_span

_LOGGING_CONFIGURED = False


class TraceContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        span = get_current_span()
        span_context = span.get_span_context()

        record.trace_id = (
            format(span_context.trace_id, "032x") if span_context.trace_id else "-"
        )
        record.span_id = (
            format(span_context.span_id, "016x") if span_context.span_id else "-"
        )
        return True


def setup_logging() -> None:
    global _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return

    settings = get_settings()

    logging.basicConfig(
        level=settings.log_level,
        format=(
            "%(asctime)s %(levelname)s %(name)s "
            "trace_id=%(trace_id)s span_id=%(span_id)s %(message)s"
        ),
    )

    logging.getLogger().addFilter(TraceContextFilter())
    _LOGGING_CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
