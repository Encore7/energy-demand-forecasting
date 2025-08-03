# ⚡ Energy Demand Forecasting

This is an end-to-end machine learning project to forecast hourly/daily energy consumption using Temporal Fusion Transformer (TFT), built with modern MLOps practices.

### Features

- PyTorch Lightning + TFT
- FastAPI for batch + real-time prediction
- Feast (feature store)
- dbt (data transformations)
- Airflow (batch orchestration)
- MLflow (experiment tracking)
- OpenTelemetry + Grafana Cloud (monitoring)

### 🔧 Local Dev Stack

Powered by `docker-compose`:
- FastAPI
- PostgreSQL
- Redis
- MLflow
- Airflow
- dbt
- Grafana Cloud (remote)

> See `.env.example` and `Makefile` for local setup.
