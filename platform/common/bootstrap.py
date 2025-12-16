from platform.common.logging import setup_logging
from platform.common.metrics import setup_metrics
from platform.common.tracing import setup_tracing


def bootstrap() -> None:
    """
    Initialize platform defaults for any runtime:
    - APIs (FastAPI)
    - batch jobs
    - Flink jobs
    - Airflow tasks

    Safe to call multiple times.
    """
    setup_logging()
    setup_tracing()
    setup_metrics()
