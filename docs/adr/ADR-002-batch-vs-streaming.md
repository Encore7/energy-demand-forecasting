# ADR-002: Batch vs Streaming

## Decision
- Batch pipelines for day-ahead
- Streaming pipelines for intraday

## Rationale
- Day-ahead does not need low latency
- Intraday requires fresh corrections
- Mixed approach balances cost and complexity
