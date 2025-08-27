from pydantic import BaseSettings


class Settings(BaseSettings):
    OTEL_EXPORTER_OTLP_ENDPOINT: str = "http://otel-collector:4317"
    MLFLOW_URL: str = "http://mlflow:5000"
    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379
    MINIO_ENDPOINT: str = "http://minio:9000"

    class Config:
        env_file = ".env"


settings = Settings()
