# ADR-007: Intraday Forecast Cadence and Horizon

## Status
Accepted (Phase 0)

## Decision
Intraday forecasts are:
- Generated every **15 minutes**
- At **15-minute resolution**
- For a rolling **6-hour horizon**

## Rationale
- Aligns with operational grid intervals
- Balances freshness and computational cost
- Matches real-world energy forecasting practices

## Consequences
- Feature freshness is critical
- Evaluation must be horizon-aware
