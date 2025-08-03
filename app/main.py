import logging

from fastapi import FastAPI

from app.core.config import settings
from app.core.telemetry import instrument_fastapi
from app.utils.trace import _trace_attrs

app = FastAPI(title=settings.PROJECT_NAME)

instrument_fastapi(app)
logger = logging.getLogger(__name__)


@app.get("/", summary="Health check")
def root():
    logger.info("Health check", extra=_trace_attrs())
    return {"message": "Energy Forecasting API is running!"}
