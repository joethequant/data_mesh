import duckdb
from models import TimeSeriesData
from typing import List
from .base import DatabaseUtility  # Assuming the base class is in the file database_utility.py


class DuckDBUtility(DatabaseUtility): 
    def __init__(self, uri: str, db_name: str = None):
        self.uri = uri
        # self.table _name = db_name        
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

    def create_table_if_not_exists(self):
        if not self.con:
            self.connect()

        self.con.execute('''
                CREATE TABLE IF NOT EXISTS timeseries_data (
                timestamp TIMESTAMP,
                source_id VARCHAR,
                series_id VARCHAR,
                value DOUBLE,
                notes VARCHAR,
                year INTEGER,
                month INTEGER,
                day INTEGER,

                PRIMARY KEY (timestamp, source_id, series_id))
        ''')

        if self.con:
            self.close()


    def upsert_time_series_data(self, data_to_upsert: List[TimeSeriesData]):

        # Ensure connection is open
        # if not self.con:
        self.connect()

        try:
            for record in data_to_upsert:
                # Check if the record exists
                exists_query = "SELECT COUNT(*) FROM timeseries_data WHERE timestamp = ? and source_id = ? and series_id = ?"
                exists = self.con.execute(exists_query, (record.timestamp, record.source_id, record.series_id)).fetchone()[0] > 0
                
                if exists:
                    # Update existing record
                    update_query = "UPDATE timeseries_data SET value = ?, notes = ?, year = ?, month = ?, day = ? WHERE timestamp = ? and source_id = ? and series_id = ?"
                    self.con.execute(update_query, (record.value, record.notes, record.year, record.month, record.day, record.timestamp, record.source_id, record.series_id))
                else:
                    # Insert new record
                    insert_query = "INSERT INTO timeseries_data (value, notes, timestamp, year, month, day, source_id, series_id ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
                    self.con.execute(insert_query, (record.value, record.notes, record.timestamp, record.year, record.month, record.day, record.source_id, record.series_id))
        except Exception as e:
            # Handle or log the error as appropriate
            raise e
        
        if self.con:
            self.close()
