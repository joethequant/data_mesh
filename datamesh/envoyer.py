from db import MongoDBUtility
from data_sources import BaseDataSource
import pandas as pd




from abc import ABC, abstractmethod
import pandas as pd
import pyarrow as pa


class DataTransformer(ABC):
    @abstractmethod
    def transform(self, data):
        pass

class PandasTransformer(DataTransformer):
    def transform(self, data):
        data_dicts = [item.dict() for item in data.data]
    
            # Create a DataFrame from the list of dictionaries
        df = pd.DataFrame(data_dicts)
            
        df['source'] = data.metadata['source']
        df['series_id'] = data.metadata['series_id']

        return df

class PyArrowTransformer(DataTransformer):
    def transform(self, data):
        data_dicts = [item.dict() for item in data.data]
    
            # Create a DataFrame from the list of dictionaries
        df = pd.DataFrame(data_dicts)
            
        df['source'] = data.metadata['source']
        df['series_id'] = data.metadata['series_id']

        return pa.Table.from_pandas(df)

class DataTransformerFactory:
    @staticmethod
    def create_transformer(type):
        if type == 'pandas':
            return PandasTransformer()
        elif type == 'pyarrow':
            return PyArrowTransformer()
        else:
            raise ValueError("Invalid type")
        


class Envoyer:
    def __init__(self, mongo_uri=None, db_name=None):
        self.data_sources = {}
        self.mongo_util = MongoDBUtility(mongo_uri, db_name) if mongo_uri and db_name else None

    def register_data_source(self, name: str, data_source: BaseDataSource):
        """Enhanced to optionally inject MongoDB utility."""
        if self.mongo_util:
            data_source.mongo_util = self.mongo_util
        self.data_sources[name] = data_source

    def fetch_data(self, source_name: str, save_to_db=False, collection_name=None, **kwargs):
        """Fetch data from a registered data source, with an option to save."""
        data_source = self.data_sources.get(source_name)
        if not data_source:
            raise ValueError(f"Data source {source_name} not registered.")

        fetch_response = data_source.fetch_data(**kwargs)
        
        if save_to_db and collection_name:
            data_source.save_data(collection_name, fetch_response)


        self.fetch_response = fetch_response

        return self
        
        # # return fetch_response

        # data_dicts = [item.dict() for item in fetch_response.data]
    
        #     # Create a DataFrame from the list of dictionaries
        # df = pd.DataFrame(data_dicts)
            
        #     # Optionally, you can include metadata as a column or use it in some other way
        #     # For example, adding a 'source' and 'series_id' column from the metadata
        # df['source'] = fetch_response.metadata['source']
        # df['series_id'] = fetch_response.metadata['series_id']


        # # data_dicts = [item.dict() for item in data]  # Convert Pydantic models to dicts
        # return df


    def to_format(self, format):

        data = self.fetch_response

        transformer = DataTransformerFactory.create_transformer(format)
        return transformer.transform(data)

    def save_to_db(self, data, collection_name):
        # Implement this method to save data to MongoDB
        pass