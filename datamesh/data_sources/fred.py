# import requests
from data_sources import BaseDataSource
    
import requests
from typing import Any, Dict
from models import TimeSeriesData, FetchResponse
import datetime

class FredDataSource(BaseDataSource):
    def __init__(self, source_id: str, api_key: str):
        self.source_id = source_id
        self.api_key = api_key
        self.base_url = "https://api.stlouisfed.org/fred/series/observations"

    def fetch_data(self, series_id: str, **kwargs) -> FetchResponse:
        params = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json",
            **kwargs
        }
        response = requests.get(self.base_url, params=params)
        response.raise_for_status()  # This will raise an error for non-2xx responses

        raw_data = response.json()['observations']

        data_points = [
            TimeSeriesData(timestamp=datetime.datetime.strptime(obs['date'], '%Y-%m-%d'),
                           source_id=self.source_id,
                           series_id=series_id,
                           year=datetime.datetime.strptime(obs['date'], '%Y-%m-%d').year,
                           month=datetime.datetime.strptime(obs['date'], '%Y-%m-%d').month,
                           day=datetime.datetime.strptime(obs['date'], '%Y-%m-%d').day,

                           value=float(obs['value']) if obs['value'] != '.' else None, 
                           notes="FRED observation")
            for obs in raw_data if obs['value'] != '.'  # Filtering out missing values
        ]

        # Assuming 'series_id' and response metadata can be directly passed as metadata
        fetch_response = FetchResponse(metadata={"source": "FRED", "series_id": series_id}, data=data_points)

        return fetch_response