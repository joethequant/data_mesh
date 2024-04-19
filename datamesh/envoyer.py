from db import MongoDBUtility
from data_sources import BaseDataSource
# from data_sources import BaseDataSource
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
    def __init__(self, db_utility=None, db_name=None):
        self.data_sources = {}
        self.db_utility = db_utility
        # self.mongo_util = MongoDBUtility(mongo_uri, db_name) if mongo_uri and db_name else None

    def register_data_source(self, name: str, data_source: BaseDataSource):
        """Enhanced to optionally inject MongoDB utility."""
        if self.db_utility:
            self.db_utility.create_table_if_not_exists()
        self.data_sources[name] = data_source

    def fetch_data(self, source_name: str, save_to_db=False, collection_name=None, **kwargs):
        """Fetch data from a registered data source, with an option to save."""
        data_source = self.data_sources.get(source_name)

        if not data_source:
            raise ValueError(f"Data source {source_name} not registered.")

        fetched_data = data_source.fetch_data(**kwargs)
        
        self.fetched_data = fetched_data

        if save_to_db:
            if not self.db_utility:
                raise ValueError(f"Database utility not provided. Cannot save data to database.")
        
            data = self.fetched_data.data
            # Implement this method to save data to database using the database utility
            self.db_utility.upsert_time_series_data(data)

        return self
        

    def to_format(self, format):

        data = self.fetched_data

        transformer = DataTransformerFactory.create_transformer(format)
        return transformer.transform(data)

