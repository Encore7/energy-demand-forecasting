from pydantic import BaseSettings


class Settings(BaseSettings):
    HOST_IP: str = "host.docker.internal"
    PROJECT_NAME: str = "energy-forecasting"
    TZ: str = "Europe/Berlin"

    OTEL_EXPORTER_OTLP_ENDPOINT: str = "http://otel-collector:4317"

    class Config:
        env_file = ".env"


settings = Settings()
