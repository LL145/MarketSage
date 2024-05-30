# src/data_sources/yfinance_data_source.py
from data_sources.base_data_source import BaseDataSource
from datetime import datetime
import yfinance as yf
import pandas as pd
import logging

class YfinanceDataSource(BaseDataSource):
    def __init__(self, client=None):
        self.client = client or yf

    def fetch_data(self, symbol: str, column_name_remote: str, column_name: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        try:
            data = self.client.download(symbol, start=start_date, end=end_date)
            print(data)
            data = data[[column_name_remote]].rename(columns={column_name_remote: column_name})
            data.index.rename('DATE', inplace=True)
            logging.info(f"{column_name} data has been fetched from Yahoo Finance.")
            print(data)
            return data
        except Exception as e:
            logging.error(f"Error fetching {column_name} data from Yahoo Finance: {e}")
            return pd.DataFrame()