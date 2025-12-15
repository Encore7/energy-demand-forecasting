# Energy Demand Forecasting Platform (Day-Ahead & Intraday)

## Overview
This repository contains a **production-grade energy demand forecasting platform**.

The system supports two forecasting horizons:
- **Day-Ahead** forecasts for planning and market operations
- **Intraday** forecasts for low-latency operational corrections

Both horizons are implemented as part of **one unified ML platform**.

---

## Design Goals
- Realistic production architecture
- Clear service boundaries
- Full observability (logs, metrics, traces)
- Automated training and retraining
- Drift detection with explicit actions
- Local development, cloud-ready deployment

This is **not a research project**.
It is a **production system simulation**.

---

## Forecasting Horizons

### Day-Ahead Forecasting
- Horizon: 24–48 hours
- Update cadence: Daily
- Latency tolerance: Minutes
- Purpose: Planning, market bidding

### Intraday Forecasting
- Horizon: 15 minutes to 6 hours
- Update cadence: Every 5–15 minutes
- Latency tolerance: Seconds
- Purpose: Operational correction and stability

Intraday forecasts **correct the day-ahead baseline**, they do not replace it.

---

## System Design

<p align="center">
  <img src="system-design.svg" alt="System Design Diagram" width="100%">
</p>

---

## Technology Choices

| Layer | Technology | Reason |
|---|---|---|
| Object storage | MinIO (S3-compatible) | Local-first, cloud portable |
| Lakehouse | Apache Iceberg | Industry standard, open |
| Metadata / SQL | PostgreSQL | Simple, reliable |
| Streaming backbone | Redpanda (Kafka API) | Kafka semantics, no ZooKeeper |
| Intraday aggregation | **Apache Flink** | Event-time, state, lateness |
| Batch compute | Spark | Feature engineering |
| Online store | Redis | Low-latency reads |
| Orchestration | Airflow | Industry-standard DAGs |
| ML tracking | MLflow | Experiments + registry |
| Observability | OpenTelemetry + Grafana stack | Unified telemetry |

---

## Why Flink for Intraday
Intraday forecasting requires:
- Stateful windowed aggregation
- Event-time correctness
- Bounded lateness handling
- Fault-tolerant state recovery

Apache Flink provides these **natively**, avoiding custom re-implementation.

Flink is **used only for intraday aggregation**.
Batch workloads remain on Spark.

---

## Observability
All services emit:
- **Traces** (request + pipeline spans)
- **Metrics** (latency, throughput, freshness, model quality)
- **Logs** with trace/span correlation

Telemetry flow:
Service → OTEL SDK → OTEL Collector → Grafana (Tempo, Loki, Prometheus)

---

## Orchestration & MLOps
Airflow DAGs automate:
- Data ingestion
- Feature generation
- Day-ahead training and backtesting
- Model promotion
- Batch forecasting
- Drift checks and automated actions

---

## Drift Detection & Actions
Drift is monitored for:
- Feature distributions
- Prediction residuals
- Data freshness and volume

Actions include:
- Alerting
- Shadow evaluation
- Rollback to last approved model
- Triggered retraining

---

## Repository Structure
- `platform/` – shared production utilities
- `services/` – microservice-style components
- `ml/` – reusable ML logic
- `infra/` – local-first infrastructure
- `monitoring/` – dashboards, alerts, SLOs
- `docs/` – ADRs, architecture, runbooks

---

## Local-First, Cloud-Ready
All services are containerized.
Infrastructure is configurable via environment variables.

Deployment to cloud platforms requires **no code changes**:
only infrastructure configuration.

---

## Disclaimer
This repository is a professional portfolio project.
It does not represent a real utility deployment.

---

## Author
Aman Kumar