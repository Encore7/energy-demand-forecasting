# Architecture Overview

## Purpose
This document describes the **concrete system architecture** of the energy
demand forecasting platform.

It answers:
- What components exist
- How data flows through the system
- How batch and streaming paths are separated
- Where models are trained and served
- How observability is wired

Design rationale and trade-offs are intentionally excluded.
Those are documented in `system-design.md` and ADRs.

---

## Architecture Diagram

![Energy Demand Forecasting Architecture](../system-design.svg)

The diagram shows the full end-to-end platform, including:
- batch (day-ahead) pipelines
- streaming (intraday) pipelines
- feature stores
- model training and inference
- observability infrastructure

---

## Data Contracts & Time Semantics

- All timestamps normalized to Europe/Berlin
- DST transitions handled explicitly
- Intraday features computed in event time
- Point-in-time correctness enforced for training and inference

---

## Major Components

### Data Sources
- Historical demand / load data
- Real-time demand events
- Weather (current and forecast)
- Calendar and holidays
- Market signals (day-ahead and intraday)

---

### Ingestion Layer
**Ingestion Service**
- Pulls or receives raw data
- Validates schemas and basic quality
- Publishes events to Kafka-compatible streaming backbone
- Writes raw batch data to object storage (bronze layer)

---

### Streaming Backbone
**Kafka-compatible log (Redpanda)**
- Decouples producers and consumers
- Provides replayability
- Serves as the source for intraday processing

---

### Batch Processing Path (Day-Ahead)
- Raw data stored in object storage (bronze)
- PySpark jobs clean and align data (silver)
- Feature engineering produces training-ready datasets (gold)
- Gold features are written to:
  - Feast Offline Store (Parquet)
- Batch inference produces precomputed day-ahead forecasts
- Forecasts are stored and exposed via API

---

### Streaming Processing Path (Intraday)
- Demand events consumed from Kafka
- Apache Flink performs:
  - event-time windowing
  - stateful aggregation
  - bounded lateness handling
- Aggregated features are written to:
  - Feast Online Store (Redis)
- Intraday forecast service reads online features
- Near-real-time forecasts are generated and published

---

### Feature Layer
**Feature Service**
- Manages offline features for training
- Manages online features for low-latency inference
- Enforces feature contracts

---

### Model Training & Registry
- Training jobs executed via batch compute
- Models tracked and versioned in MLflow
- Approved models are registered for inference

---

### Forecast Services
- **Day-Ahead Forecast Service**
  - Serves precomputed forecasts
- **Intraday Forecast Service**
  - Serves near-real-time predictions

---

### API Gateway
- Single external entry point
- Routes requests to appropriate forecast services
- Exposes health and metrics endpoints

---

### Observability
All services emit telemetry to a centralized OpenTelemetry Collector:
- Metrics → Prometheus
- Logs → Loki
- Traces → Tempo

Grafana provides dashboards and alerting.

---

## Scope
This architecture reflects a **production-style platform** suitable for
utilities or energy-focused data teams.
