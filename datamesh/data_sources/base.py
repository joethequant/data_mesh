
from pydantic import BaseModel, Field
from typing import List, Optional
import datetime
from abc import ABC, abstractmethod
from models import FetchResponse
from db import MongoDBUtility
import pandas as pd



class DataTransformer(ABC):
    @abstractmethod
    def transform(self, data):
        pass

class PandasTransformer(DataTransformer):
    def transform(self, data):
        return pd.DataFrame(data)

class PyArrowTransformer(DataTransformer):
    def transform(self, data):
        return pa.Table.from_pandas(pd.DataFrame(data))

class DataTransformerFactory:
    @staticmethod
    def create_transformer(type):
        if type == 'pandas':
            return PandasTransformer()
        elif type == 'pyarrow':
            return PyArrowTransformer()
        else:
            raise ValueError("Invalid type")
        


class BaseDataSource(ABC):

    def __init__(self, mongodb_uri: str, db_name: str):
        # Initialize MongoDB utility
        self.mongo_util = MongoDBUtility(mongodb_uri, db_name)

    @abstractmethod
    def fetch_data(self, **kwargs) -> FetchResponse:
        """
        Fetch data from the data source. This method must be implemented by subclasses.
        
        The implementation must return a FetchResponse object or data that can be parsed into one,
        ensuring that the returned data conforms to the defined schema.
        """
        pass

    def to_dataframe(self, data):
        """Convert a list of TimeSeriesData models into a pandas DataFrame."""
        data_dicts = [item.dict() for item in data]  # Convert Pydantic models to dicts
        return pd.DataFrame(data_dicts)

    def save_data(self, collection_name: str, data):
        if not self.mongo_util:
            raise ValueError("MongoDB utility not configured for this data source.")
        # Assuming data is a list of Pydantic models
        data_dicts = [datum.dict() for datum in data]
        self.mongo_util.insert_time_series_data(collection_name, data_dicts)



        