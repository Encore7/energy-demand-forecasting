from fastapi import APIRouter

from app.api.endpoints import forecast, health

api_router = APIRouter()
api_router.include_router(health.router, tags=["health"])
api_router.include_router(forecast.router, tags=["forecast"])
