# ADR-001: Service Boundaries

## Decision
Adopt a **microservice-style architecture** with clear responsibility boundaries
aligned to data lifecycle and forecasting horizons.

## Services (conceptual)
- ingestion-service
- streaming-feature-service
- batch-feature-service
- training-service
- intraday-inference-service
- dayahead-inference-service
- api-gateway

## Rationale
- Independent scaling and failure isolation
- Horizon-specific optimization (day-ahead vs intraday)
- Clear ownership and operability
- Mirrors real production ML platforms in energy systems

## Consequences
- Services communicate only via explicit data contracts
- Forecasting horizons never share mutable state
- Feature definitions are centralized but materialized separately
