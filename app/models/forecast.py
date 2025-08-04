from datetime import datetime

from pydantic import BaseModel, Field


class ForecastRequest(BaseModel):
    """
    Represents a request for weather forecast data.
    Attributes:
        region: The geographical region for which the forecast is requested.
        timestamp: The date and time for which the forecast is requested.
        temperature: The expected temperature at the specified time.
        humidity: The expected humidity at the specified time.
        is_holiday: Indicates if the date is a holiday.
        day_of_week: The day of the week for the specified date.
    """

    region: str = Field(..., example="berlin_north")
    timestamp: datetime = Field(..., example="2025-08-03T10:00:00Z")
    temperature: float = Field(..., example=26.5)
    humidity: float = Field(..., example=55.0)
    is_holiday: bool = Field(..., example=False)
    day_of_week: str = Field(..., example="Sunday")
