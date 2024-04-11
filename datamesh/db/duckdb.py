import duckdb
from models import TimeSeriesData
from typing import List
from .base import DatabaseUtility  # Assuming the base class is in the file database_utility.py


class DuckDBUtility(DatabaseUtility): 
    def __init__(self, uri: str, db_name: str):
        self.uri = uri
        self.db_name = db_name        
        self.con = None

    # Context manager support
    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect(self):
        self.con = duckdb.connect(self.uri)

    def close(self):
        if self.con:
            self.con.close()

    def upsert_time_series_data(self, data_to_upsert: List[TimeSeriesData]):

        # Ensure connection is open
        if not self.con:
            self.connect()

        try:
            for record in data_to_upsert:
                # Check if the record exists
                exists_query = "SELECT COUNT(*) FROM timeseries_data WHERE timestamp = ? and source = ? and series_id = ?"
                exists = self.con.execute(exists_query, (record.timestamp, record.source_id, record.series_id)).fetchone()[0] > 0
                
                if exists:
                    # Update existing record
                    update_query = "UPDATE timeseries_data SET value = ?, notes = ?, year = ?, month = ?, day = ? WHERE timestamp = ? and source = ? and series_id = ?"
                    self.con.execute(update_query, (record.value, record.notes, record.year, record.month, record.day, record.timestamp, record.source_id, record.series_id))
                else:
                    # Insert new record
                    insert_query = "INSERT INTO timeseries_data (value, notes, timestamp, year, month, day, source, series_id ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
                    self.con.execute(insert_query, (record.value, record.notes, record.timestamp, record.year, record.month, record.day, record.source_id, record.series_id))
        except Exception as e:
            # Handle or log the error as appropriate
            raise e
        
        if self.con:
            self.close()



# from pymongo import MongoClient
# from typing import List

# class MongoDBUtility:
#     def __init__(self, uri: str, db_name: str):
#         self.client = MongoClient(uri)
#         self.db = self.client[db_name]

#     def insert_time_series_data(self, collection_name: str, data: List[dict]):
#         collection = self.db[collection_name]
#         result = collection.insert_many(data)
#         return result.inserted_ids

    # Add more utility methods as needed, such as for updating or querying data





# import duckdb
# from models import TimeSeriesData, FetchResponse
# import datetime


# # Function to simulate upsert behavior
# def upsert_data(duckdb_con, data_to_upsert: list[TimeSeriesData]):
#     for record in data_to_upsert:
#         # Check if the record exists
#         exists_query = "SELECT COUNT(*) FROM timeseries_data WHERE timestamp = ? and source = ? and series_id = ?"
#         exists = duckdb_con.execute(exists_query, (record.timestamp, record.source, record.series_id)).fetchone()[0] > 0
        
#         if exists:
#             # Update existing record
#             update_query = "UPDATE timeseries_data SET value = ?, notes = ?, year = ?, month = ?, day = ? WHERE timestamp = ? and source = ? and series_id = ?"
#             duckdb_con.execute(update_query, (record.value, record.notes, record.year, record.month, record.day, record.timestamp, record.source, record.series_id))
#         else:
#             # Insert new record
#             insert_query = "INSERT INTO timeseries_data (value, notes, timestamp, year, month, day, source, series_id ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
#             duckdb_con.execute(insert_query, (record.value, record.notes, record.timestamp, record.year, record.month, record.day, record.source, record.series_id))

# # Example usage
# duckdb_con = duckdb.connect('FRED.duckdb')

# # Assuming `data_to_upsert` is populated with TimeSeriesData instances
# data_to_upsert = [
#     TimeSeriesData(timestamp=datetime.datetime.today(), year = 2020, month = 12, day = 1, source='FRED', series_id='hi', value=150.0, notes="Example note")
# ]

# duckdb_con.close()