# ⚡ Real-Time Energy Demand Forecasting System

A production-ready, **local-first** machine learning system for forecasting **Germany's energy demand** in:
- **Day-Ahead (DA)** market (24×1h forecast, once daily)
- **Intraday (ID)** market (rolling forecasts every 5–15 minutes)

Designed to run locally via **Docker Compose** for staging, but fully portable to **Kubernetes** or cloud environments.

---

## Architecture Overview

The system has two independent ML pipelines:

### 1. Day-Ahead (DA) — Batch (PySpark)
- **Cadence:** Once per day at cutoff time
- **Features:** Slow-changing (yesterday’s load, tomorrow’s weather forecast, calendar)
- **Processing:** PySpark batch jobs (Bronze → Silver → Gold DA)
- **Feature Store:** Feast **Offline** (Parquet in MinIO/lakeFS)
- **Serving:** Precomputes next day’s forecast → stores in Parquet + Kafka → served via `da-svc` (FastAPI) instantly
- **Model:** `model_da` in MLflow Registry

### 2. Intraday (ID) — Streaming (PyFlink)
- **Cadence:** Continuous; updates every 5–15 min
- **Features:** Fast-changing (current load, latest weather nowcasts, updated market prices)
- **Processing:** PyFlink streaming jobs → real-time feature calculation
- **Feature Store:** Feast **Online** (Redis)
- **Serving:** `id-svc` (FastAPI) pulls features from Redis → low-latency scoring in memory
- **Model:** `model_id` in MLflow Registry

---

## System Design

<p align="center">
  <img src="system-design.svg" alt="System Design Diagram" width="100%">
</p>

---

## Data Flow (high level)

- **DA Path:**  
  Bronze → PySpark (Silver) → PySpark (Gold DA) → Feast Offline → MLflow model → Batch inference → Stored forecast → API + Grafana
- **ID Path:**  
  Kafka → PyFlink → Feast Online (Redis) → FastAPI inference → Forecast → Grafana

---

## 🛠️ Tech Stack

| Layer                | Tooling |
|----------------------|---------|
| **Messaging**        | Kafka, Karapace Schema Registry |
| **Batch Processing** | PySpark |
| **Stream Processing**| PyFlink |
| **Storage**          | MinIO (S3 API), lakeFS (versioning), Parquet |
| **Feature Store**    | Feast (Offline = Parquet, Online = Redis) |
| **Model Registry**   | MLflow Tracking + Registry |
| **Orchestration**    | Airflow |
| **Validation**       | Great Expectations |
| **Drift Monitoring** | Evidently |
| **Serving**          | FastAPI (`da-svc`, `id-svc`) |
| **Observability**    | OpenTelemetry → Prometheus (metrics), Loki (logs), Tempo (traces), Grafana (dashboards + alerts) |

---

## Pipelines

### Day-Ahead (DA)
1. **Ingestion:** Data arrives in Kafka → Lakehouse (Bronze)
2. **Batch Prep:** PySpark → Silver → Gold DA
3. **Validation:** Great Expectations on Silver/Gold
4. **Feature Store:** Write Gold features to Feast Offline
5. **Training:** Run daily/weekly; log to MLflow
6. **Inference:** Load `model_da` from MLflow → predict → store forecast in Parquet + Kafka
7. **Serving:** `da-svc` returns precomputed forecast

### Intraday (ID)
1. **Ingestion:** Data arrives in Kafka
2. **Stream Processing:** PyFlink calculates real-time features → writes to Redis (Feast Online)
3. **Serving:** `id-svc` pulls features from Redis → runs `model_id` in memory → returns predictions
4. **(Optional)** Store ID forecasts in Kafka/Parquet for audit and dashboarding

---

## Models

- **`model_da`** — Day-Ahead  
  Trained on Gold DA features from Feast Offline

- **`model_id`** — Intraday  
  Trained on historical Gold ID features backfilled from PyFlink outputs

Both are versioned and promoted in **MLflow Registry** with guardrails.

---

## Observability

- **Metrics:** Prometheus (pipeline durations, API latency, drift metrics)
- **Logs:** Loki
- **Traces:** Tempo (end-to-end spans from ingestion → serving)
- **Dashboards & Alerts:** Grafana