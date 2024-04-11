from abc import ABC, abstractmethod
from typing import List

class DatabaseUtility(ABC):
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def upsert_time_series_data(self, data):
        pass
