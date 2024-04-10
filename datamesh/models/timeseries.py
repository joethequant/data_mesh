from pydantic import BaseModel, Field
from typing import List, Optional
import datetime

class TimeSeriesData(BaseModel):
    timestamp: datetime.datetime
    source: str
    series_id: str
    year: int
    month: int
    day: int
    value: float
    notes: Optional[str] = Field(None, description="Optional notes about the observation.")

class FetchResponse(BaseModel):
    metadata: dict
    data: List[TimeSeriesData]