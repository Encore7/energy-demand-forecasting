# ADR-002: Batch vs Streaming

## Decision
- **Batch pipelines** are used for:
  - day-ahead feature generation
  - training data preparation
  - daily day-ahead inference
- **Streaming pipelines** are used for:
  - intraday feature materialization
  - low-latency freshness guarantees

Inference itself is **not fully streaming**.

## Rationale
- Day-ahead forecasting is deadline-driven and reproducible
- Intraday forecasting requires fresh features but benefits from deterministic inference
- Streaming inference on every event adds complexity without operational value

## Locked Invariant
> Streaming is used to keep features fresh.  
> Inference runs every 15 minutes on a fixed schedule.
