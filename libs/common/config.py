from pydantic import BaseSettings


class Settings(BaseSettings):
    HOST_IP: str = "host.docker.internal"
    PROJECT_NAME: str = "energy-forecasting"
    TZ: str = "Europe/Berlin"

    OTEL_EXPORTER_OTLP_ENDPOINT: str = "http://otel-collector:4317"

    MINIO_ROOT_USER: str = "minio"
    MINIO_ROOT_PASSWORD: str = "minio123"
    MINIO_MLFLOW_BUCKET: str = "mlflow"

    # lakeFS
    LAKEFS_DB_USER: str = "lakefs"
    LAKEFS_DB_PASSWORD: str = "lakefs"
    LAKEFS_DB_NAME: str = "lakefs"
    LAKEFS_SECRET_KEY: str

    # MLflow DB
    MLFLOW_DB_USER: str = "mlflow"
    MLFLOW_DB_PASSWORD: str = "mlflow"
    MLFLOW_DB_NAME: str = "mlflow"

    # Airflow DB + Admin
    AIRFLOW_DB_USER: str = "airflow"
    AIRFLOW_DB_PASSWORD: str = "airflow"
    AIRFLOW_DB_NAME: str = "airflow"
    AIRFLOW_ADMIN_USERNAME: str = "admin"
    AIRFLOW_ADMIN_PASSWORD: str = "admin"

    # -------- Grafana Cloud OTLP (Collector will use these) --------
    OTLP_ENDPOINT: str
    OTLP_TOKEN: str

    class Config:
        env_file = ".env"


settings = Settings()
