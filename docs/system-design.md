# System Design Rationale

## Purpose
This document explains the **design decisions and trade-offs** behind the
architecture.

It answers:
- Why specific technologies were chosen
- Why responsibilities are separated
- How correctness, latency, and operability are balanced
- How the system is expected to evolve

For a visual overview, refer to `system-design.svg`
and `architecture.md`.

---

## Problem Framing

This system forecasts Germany-wide electricity demand with two horizons:
- Day-ahead (hourly, batch)
- Intraday (15-min resolution, refreshed every 15 minutes, 6h horizon)

Forecasts are probabilistic by default (P10/P50/P90) and evaluated via
rolling-origin backtesting using pinball loss.

---

## Why Two Forecasting Horizons

### Day-Ahead
- Supports planning and market operations
- Requires stability and reproducibility
- Latency tolerance is minutes

### Intraday
- Supports operational correction
- Requires low-latency updates
- Continuously corrects the day-ahead baseline

Separating horizons allows each to be optimized independently.

---

## Why Batch + Streaming

Batch and streaming pipelines solve fundamentally different problems:
- Batch processing optimizes for correctness and reproducibility
- Streaming processing optimizes for freshness and responsiveness

Combining both avoids overloading a single paradigm.

---

## Forecasting Execution Model

- Streaming pipelines are used to continuously materialize features.
- Intraday inference runs on a fixed **15-minute schedule**, not per event.
- This design ensures deterministic outputs, simpler rollback, and auditable forecasts.

---

## Why Spark for Batch
- Mature ecosystem for large-scale data processing
- Strong support for feature engineering
- Reproducible, deterministic pipelines
- Well-suited for daily training and scoring workloads

Streaming micro-batch systems were intentionally avoided for batch workloads.

---

## Why Flink for Intraday
Intraday forecasting requires:
- Event-time semantics
- Windowed aggregation
- Stateful processing
- Bounded lateness handling
- Fault-tolerant recovery

Apache Flink provides these capabilities natively.
Implementing them manually would reintroduce complexity and risk.

Flink is used **only** where these guarantees are required.

---

## Why Kafka-Compatible Streaming
- Decouples ingestion from processing
- Allows replay and backfill
- Enables independent scaling of producers and consumers

Kafka semantics are preserved while simplifying local operations.

---

## Why Separate Offline and Online Features
- Offline features ensure training–serving consistency
- Online features ensure low-latency inference
- Clear separation reduces accidental coupling

Feature definitions are treated as contracts.

---

## Why MLflow
- Central experiment tracking
- Versioned model registry
- Explicit promotion and rollback points
- Integration with batch training workflows

---

## Evaluation & Backtesting Strategy

- Rolling-origin evaluation
- Leakage-safe feature alignment
- Primary metric: pinball loss (P10/P50/P90)
- Secondary: MAE, RMSE, sMAPE
- Segment-based evaluation (peaks, seasons, holidays)

---

## Why Observability is First-Class
Forecasting systems fail silently without observability.

OpenTelemetry enables:
- End-to-end traceability across services
- Correlation of data issues with model issues
- Faster root-cause analysis

Logs, metrics, and traces are treated as mandatory outputs.

---

## Failure Handling Philosophy
The system assumes failures will occur:
- data delays
- model degradation
- streaming job restarts

Failures are handled via:
- explicit runbooks
- fallback to last-known-good forecasts
- rollback through model registry
- retraining triggers

---

## Scalability & Evolution
The system is designed to evolve:
- Streaming complexity can grow without affecting batch pipelines
- Cloud-managed services can replace local components
- Additional regions or markets can be added by partitioning

The architecture avoids premature optimization while preserving future paths.

---

## Non-Goals
- Ultra-low-latency trading systems
- Regulatory compliance tooling
- Market bidding automation

Those concerns are intentionally out of scope.
