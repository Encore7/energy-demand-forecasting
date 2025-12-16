class AppError(Exception):
    """Base application error."""


class IngestionError(AppError):
    """Raised when ingestion fails."""


class FeatureComputationError(AppError):
    """Raised when feature computation fails."""


class ModelInferenceError(AppError):
    """Raised when inference fails."""


class ExternalDependencyError(AppError):
    """Raised when an external dependency (API/DB/Kafka) fails."""
