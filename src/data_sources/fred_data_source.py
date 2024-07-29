# src/data_sources/fred_data_source.py
from data_sources.base_data_source import BaseDataSource
from datetime import datetime
from pandas_datareader import data as pdr
from config_reader import ConfigReader
import pandas as pd
import logging

class FredDataSource(BaseDataSource):
    def __init__(self, config_reader: ConfigReader, client = None):
        self.client = client or pdr
        self.config = config_reader

    def fetch_data(self, symbol: str, column_name_remote: str, column_name: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        try:
            data = self.client.get_data_fred(symbol, start=start_date, end=end_date)
            data = data[[column_name_remote]].rename(columns={column_name_remote: column_name})
            data.index.rename('DATE', inplace=True)
            data_config_map = self.config.get_data_config_map()
            if data_config_map[column_name]['freq'] == 'M':
                data.index = data.index + pd.DateOffset(months=1)
            if data_config_map[column_name]['freq'] == 'Q':
                data.index = data.index + pd.DateOffset(months=3)
            # 房价数据再往后移一个月。。。
            if symbol == 'CSUSHPINSA':
                data.index = data.index + pd.DateOffset(months=1)
            logging.info(f"{column_name} data has been fetched from FRED.")
            return data
        except Exception as e:
            logging.error(f"Error fetching {column_name} data from FRED: {e}")
            return pd.DataFrame()