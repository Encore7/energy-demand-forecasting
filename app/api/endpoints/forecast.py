import logging

from fastapi import APIRouter

from app.models.forecast import ForecastRequest
from app.models.response import ForecastResponse
from app.utils.trace import _trace_attrs

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post(
    "/forecast/live", response_model=ForecastResponse, summary="Live energy forecast"
)
async def forecast_live(request: ForecastRequest) -> ForecastResponse:
    logger.info(
        "Received forecast request", extra={**_trace_attrs(), "region": request.region}
    )
    # TODO: DUMMY - replace with real inference
    return ForecastResponse(
        region=request.region,
        forecast={"10th": 350.2, "50th": 420.1, "90th": 510.3},
        units="kWh",
        confidence_interval="80%",
    )
