from typing import Any, Literal

from pydantic import AnyHttpUrl, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings and configuration.
    This class uses Pydantic for settings management and validation.
    It loads environment variables from a .env file and provides type-safe access
    to application settings.
    Attributes:
        PROJECT_NAME (str): Name of the project.
        ENVIRONMENT (Literal): Environment type (local, staging, production).
        VERSION (str): Application version.
        API_V1_STR (str): API versioning string.
        DATABASE_URL (str): PostgreSQL database connection URL.
        REDIS_URL (str): Redis connection URL.
        OTLP_ENDPOINT (AnyHttpUrl): Endpoint for Grafana OTLP tracing.
        OTLP_TOKEN (str): Token for Grafana OTLP tracing authentication.
    Raises:
        ValueError: If any of the settings are invalid or cannot be parsed.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_ignore_empty=True,
        extra="ignore",
    )

    # Core App Info
    PROJECT_NAME: str = "Energy Demand Forecasting"
    VERSION: str = "0.1.0"
    API_V1_STR: str = "/v1"
    ENVIRONMENT: Literal["local", "staging", "production"] = "local"

    # PostgreSQL & Redis
    DATABASE_URL: str
    REDIS_URL: str

    # Grafana OTLP
    OTLP_ENDPOINT: AnyHttpUrl
    OTLP_TOKEN: str

    @field_validator("OTLP_TOKEN")
    @classmethod
    def validate_token(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("OTLP_TOKEN must not be empty.")
        return v


settings = Settings()
