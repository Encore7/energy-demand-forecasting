from pydantic import BaseModel, Field


class ForecastResponse(BaseModel):
    """
    Response model for forecast data.
    This model represents the forecast data for a specific region,
    including the forecast values, units, and confidence interval.
    Attributes:
        region (str): The region for which the forecast is made.
        forecast (dict): A dictionary containing forecast values at different percentiles.
        units (str): The units of the forecast values, default is "kWh".
        confidence_interval (str): The confidence interval for the forecast, default is "80%".
    """

    region: str = Field(..., example="berlin_north")
    forecast: dict = Field(..., example={"10th": 350.2, "50th": 420.1, "90th": 510.3})
    units: str = Field(default="kWh")
    confidence_interval: str = Field(default="80%")
