from functools import lru_cache

from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    # General
    env: str = Field(default="local", alias="ENV")
    service_name: str = Field(default="energy-forecasting", alias="SERVICE_NAME")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    # OpenTelemetry
    otel_service_name: str = Field(
        default="energy-forecasting", alias="OTEL_SERVICE_NAME"
    )
    otel_exporter_otlp_endpoint: str = Field(
        default="http://otel-collector:4317", alias="OTEL_EXPORTER_OTLP_ENDPOINT"
    )
    otel_exporter_otlp_protocol: str = Field(
        default="grpc", alias="OTEL_EXPORTER_OTLP_PROTOCOL"
    )

    # Kafka / Redpanda
    kafka_bootstrap_servers: str = Field(
        default="redpanda:9092", alias="KAFKA_BOOTSTRAP_SERVERS"
    )
    kafka_topic_demand: str = Field(default="demand_events", alias="KAFKA_TOPIC_DEMAND")
    kafka_topic_weather: str = Field(
        default="weather_events", alias="KAFKA_TOPIC_WEATHER"
    )
    kafka_topic_market: str = Field(default="market_events", alias="KAFKA_TOPIC_MARKET")
    kafka_consumer_group: str = Field(
        default="energy-platform", alias="KAFKA_CONSUMER_GROUP"
    )
    kafka_security_protocol: str = Field(
        default="PLAINTEXT", alias="KAFKA_SECURITY_PROTOCOL"
    )

    # Schema Registry
    schema_registry_url: str = Field(
        default="http://schema-registry:8081", alias="SCHEMA_REGISTRY_URL"
    )

    # Redis (online store / caching)
    redis_host: str = Field(default="redis", alias="REDIS_HOST")
    redis_port: int = Field(default=6379, alias="REDIS_PORT")
    redis_db: int = Field(default=0, alias="REDIS_DB")
    redis_password: str | None = Field(default=None, alias="REDIS_PASSWORD")

    # Postgres (metadata, forecasts, MLflow backend)
    postgres_host: str = Field(default="postgres", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    postgres_db: str = Field(default="energy", alias="POSTGRES_DB")
    postgres_user: str = Field(default="energy", alias="POSTGRES_USER")
    postgres_password: str = Field(default="energy", alias="POSTGRES_PASSWORD")
    postgres_sslmode: str = Field(default="disable", alias="POSTGRES_SSLMODE")

    # Single DSN override
    database_url: str | None = Field(default=None, alias="DATABASE_URL")

    # Object Storage (MinIO / S3)
    object_store_endpoint: str = Field(
        default="http://minio:9000", alias="OBJECT_STORE_ENDPOINT"
    )
    object_store_access_key: str = Field(
        default="minioadmin", alias="OBJECT_STORE_ACCESS_KEY"
    )
    object_store_secret_key: str = Field(
        default="minioadmin", alias="OBJECT_STORE_SECRET_KEY"
    )
    object_store_bucket: str = Field(default="energy-lake", alias="OBJECT_STORE_BUCKET")
    object_store_region: str = Field(default="us-east-1", alias="OBJECT_STORE_REGION")
    object_store_secure: bool = Field(default=False, alias="OBJECT_STORE_SECURE")

    # Lakehouse layout (paths/prefixes)
    lakehouse_root_prefix: str = Field(
        default="lakehouse", alias="LAKEHOUSE_ROOT_PREFIX"
    )
    bronze_prefix: str = Field(default="bronze", alias="BRONZE_PREFIX")
    silver_prefix: str = Field(default="silver", alias="SILVER_PREFIX")
    gold_prefix: str = Field(default="gold", alias="GOLD_PREFIX")

    # Iceberg / Spark (batch compute)
    spark_master: str = Field(default="local[*]", alias="SPARK_MASTER")
    spark_app_name: str = Field(default="energy-platform", alias="SPARK_APP_NAME")
    iceberg_catalog_name: str = Field(default="local", alias="ICEBERG_CATALOG_NAME")
    iceberg_warehouse: str = Field(
        default="s3a://energy-lake/warehouse", alias="ICEBERG_WAREHOUSE"
    )

    # Flink (intraday aggregation)
    flink_jobmanager_url: str = Field(
        default="http://flink-jobmanager:8081", alias="FLINK_JOBMANAGER_URL"
    )
    intraday_window_minutes: int = Field(default=5, alias="INTRADAY_WINDOW_MINUTES")
    intraday_allowed_lateness_seconds: int = Field(
        default=30, alias="INTRADAY_ALLOWED_LATENESS_SECONDS"
    )

    # Feast (feature store)
    feast_repo_path: str = Field(
        default="platform/contracts/feast", alias="FEAST_REPO_PATH"
    )
    feast_online_store: str = Field(default="redis", alias="FEAST_ONLINE_STORE")
    feast_offline_store: str = Field(default="parquet", alias="FEAST_OFFLINE_STORE")

    # MLflow (tracking + registry)
    mlflow_tracking_uri: str = Field(
        default="http://mlflow:5000", alias="MLFLOW_TRACKING_URI"
    )
    mlflow_experiment_name: str = Field(
        default="energy-demand-forecasting", alias="MLFLOW_EXPERIMENT_NAME"
    )

    # Service URLs (internal routing, future service discovery)
    api_gateway_url: str = Field(
        default="http://api-gateway:8000", alias="API_GATEWAY_URL"
    )
    ingestion_service_url: str = Field(
        default="http://ingestion-service:8000", alias="INGESTION_SERVICE_URL"
    )
    feature_service_url: str = Field(
        default="http://feature-service:8000", alias="FEATURE_SERVICE_URL"
    )
    training_service_url: str = Field(
        default="http://training-service:8000", alias="TRAINING_SERVICE_URL"
    )
    realtime_forecast_service_url: str = Field(
        default="http://realtime-forecast-service:8000",
        alias="REALTIME_FORECAST_SERVICE_URL",
    )
    drift_service_url: str = Field(
        default="http://drift-service:8000", alias="DRIFT_SERVICE_URL"
    )

    # Security toggles (local-first, future cloud)
    auth_enabled: bool = Field(default=False, alias="AUTH_ENABLED")
    internal_api_key: str | None = Field(default=None, alias="INTERNAL_API_KEY")

    # Derived helpers
    @property
    def postgres_dsn(self) -> str:
        if self.database_url:
            return self.database_url
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
            f"?sslmode={self.postgres_sslmode}"
        )

    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache
def get_settings() -> Settings:
    return Settings()
