# src/data_sources/fred_data_source.py
from data_sources.base_data_source import BaseDataSource
from datetime import datetime
from pandas_datareader import data as pdr
import pandas as pd
import logging

class FredDataSource(BaseDataSource):
    def __init__(self, client=None):
        self.client = client or pdr

    def fetch_data(self, symbol: str, column_name_remote: str, column_name: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        try:
            data = self.client.get_data_fred(symbol, start=start_date, end=end_date)
            data = data[[column_name_remote]].rename(columns={column_name_remote: column_name})
            data.index.rename('DATE', inplace=True)
            if symbol in ['PPIACO', 'CPIAUCSL', 'PCE', 'PAYEMS', 'SAHMREALTIME', 'M1SL', 'M2SL', 'UNRATE', 'CSUSHPINSA', 'BOGMBASE', 'UMCSENT']:
                data.index = data.index + pd.DateOffset(months=1)
            if symbol in ['GDP', 'GDPC1']:
                data.index = data.index + pd.DateOffset(months=3)
                
            logging.info(f"{column_name} data has been fetched from FRED.")
            return data
        except Exception as e:
            logging.error(f"Error fetching {column_name} data from FRED: {e}")
            return pd.DataFrame()