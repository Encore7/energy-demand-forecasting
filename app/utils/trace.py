from opentelemetry.trace import get_current_span


def _trace_attrs() -> dict[str, str]:
    """Get the current trace attributes.
    Returns:
        dict: A dictionary containing the trace ID and span ID.
    """
    ctx = get_current_span().get_span_context()
    if not ctx or not ctx.is_valid:
        return {"trace_id": "undefined", "span_id": "undefined"}
    return {
        "trace_id": format(ctx.trace_id, "032x"),
        "span_id": format(ctx.span_id, "016x"),
    }
