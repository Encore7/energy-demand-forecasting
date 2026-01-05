# ADR-005: Observability with OpenTelemetry

## Decision
Adopt **OpenTelemetry** for logs, metrics, and traces across all services.

## Rationale
- Vendor-neutral standard
- End-to-end request and data lineage
- Required for debugging ML failures that are otherwise silent

## Consequence
No service is production-ready without telemetry.
