from abc import ABC, abstractmethod
import pandas as pd
import pyarrow as pa

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