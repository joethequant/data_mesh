from pymongo import MongoClient
from typing import List

class MongoDBUtility:
    def __init__(self, uri: str, db_name: str):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]

    def insert_time_series_data(self, collection_name: str, data: List[dict]):
        collection = self.db[collection_name]
        result = collection.insert_many(data)
        return result.inserted_ids

    # Add more utility methods as needed, such as for updating or querying data
