# Energy Demand Forecasting Platform (Day-Ahead & Intraday)

## Overview
This repository implements a **production-grade energy demand forecasting platform**
designed to mirror **real-world utility and big-tech ML systems**.

The platform supports two forecasting horizons:
- **Day-Ahead** forecasts for planning and market operations
- **Intraday** forecasts for low-latency operational correction

Both horizons are implemented as part of **one unified ML platform**
with shared data contracts, observability, and MLOps workflows.

---

## Design Goals
- Realistic production architecture
- Clear service boundaries and ownership
- Full observability (logs, metrics, traces)
- Automated training, retraining, and rollback
- Explicit drift detection and mitigation
- Local-first development with cloud-ready deployment

This is **not a research prototype**.
It is a **production system simulation** focused on operability and correctness.

---

## Forecasting Horizons

### Day-Ahead Forecasting
- Horizon: 24–48 hours  
- Update cadence: Daily  
- Latency tolerance: Minutes  
- Purpose: Planning, scheduling, market bidding  

### Intraday Forecasting
- Horizon: 15 minutes to 6 hours  
- Update cadence: Every 5–15 minutes  
- Latency tolerance: Seconds  
- Purpose: Operational correction and stability  

Intraday forecasts **correct the day-ahead baseline** rather than replacing it.

---

## System Architecture

<p align="center">
  <img src="system-design.svg" alt="System Architecture Diagram" width="100%">
</p>

The diagram shows the full end-to-end platform, including:
- batch (day-ahead) pipelines
- streaming (intraday) pipelines
- feature stores
- training and inference paths
- observability integration

A detailed architectural walkthrough is provided in:
- **`docs/architecture.md`** (what exists and how data flows)

---

## Technology Choices

| Layer | Technology | Reason |
|---|---|---|
| Object storage | MinIO (S3-compatible) | Local-first, cloud portable |
| Lakehouse | Apache Iceberg | Open, industry-standard table format |
| Metadata / SQL | PostgreSQL | Simple, reliable metadata store |
| Streaming backbone | Redpanda (Kafka API) | Kafka semantics without ZooKeeper |
| Intraday aggregation | **Apache Flink** | Event-time, stateful processing |
| Batch compute | Spark | Reproducible feature engineering |
| Online store | Redis | Low-latency feature access |
| Orchestration | Airflow | Industry-standard DAG orchestration |
| ML tracking | MLflow | Experiments, registry, rollback |
| Observability | OpenTelemetry + Grafana stack | Unified telemetry |

---

## Design Rationale (Summary)

### Why Flink for Intraday
Intraday forecasting requires:
- event-time windowed aggregation
- stateful processing
- bounded lateness handling
- fault-tolerant recovery

Apache Flink provides these guarantees natively.
It is used **only** for intraday aggregation.

Batch workloads (training and day-ahead inference) remain on Spark.

A full rationale is documented in:
- **`docs/system-design.md`**
- **`docs/adr/ADR-003-why-flink-for-intraday.md`**

---

## Observability
All services emit telemetry via OpenTelemetry:
- **Traces** for end-to-end request and pipeline visibility
- **Metrics** for latency, throughput, freshness, and model quality
- **Logs** correlated with trace and span identifiers

Telemetry flow: Service → OTEL SDK → OTEL Collector → Grafana (Tempo, Loki, Prometheus)


Observability decisions are documented in:
- **`docs/adr/ADR-005-otel-observability.md`**

---

## Orchestration & MLOps
Airflow DAGs automate:
- data ingestion
- feature generation
- day-ahead training and backtesting
- model promotion and rollback
- batch forecasting
- drift checks and retraining triggers

Operational workflows and failure handling are documented in:
- **`docs/runbooks/`**

---

## Drift Detection & Actions
The platform monitors:
- feature distribution drift
- prediction residual drift
- data freshness and volume anomalies

Supported actions:
- alerting
- shadow evaluation
- rollback to last approved model
- triggered retraining

Design details are documented in:
- **`docs/adr/ADR-006-drift-detection-and-actions.md`**

---

## Repository Structure
- `libs/` – shared production utilities (config, logging, tracing, metrics)
- `services/` – microservice-style runtime components
- `ml/` – reusable ML logic (baselines, training, evaluation)
- `infra/` – local-first infrastructure and observability stack
- `monitoring/` – dashboards, alerts, SLO definitions
- `docs/` – architecture, system design, ADRs, and runbooks

---

## Cloud-Ready(AWS/Azure/GCP)
All components are containerized and configured via environment variables.

Deploying to managed cloud services requires **no code changes** —
only infrastructure configuration.

---

## Disclaimer
This repository is a professional portfolio project.
It does not represent a real utility deployment.

---

## Author
Aman Kumar

