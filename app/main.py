import uvicorn
from fastapi import FastAPI

from app.api.router import api_router
from app.core.config import settings
from app.core.telemetry import get_logger, instrument_fastapi

# Initialize logger first
logger = get_logger(__name__)
logger.info("Starting FastAPI application...")

# FastAPI app instance
app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
    docs_url=f"{settings.API_V1_STR}/docs",
)

# Instrument FastAPI after OTel setup
instrument_fastapi(app)

# Register API routes
app.include_router(api_router, prefix=settings.API_V1_STR)

logger.info("Application initialized successfully.")

# Entrypoint
if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
