# src/data_sources/other_data_source.py
from data_sources.base_data_source import BaseDataSource
from datetime import datetime
from pandas_datareader import data as pdr
from config_reader import ConfigReader
import pandas as pd
import logging

class OtherDataSource(BaseDataSource):
    def __init__(self, config_reader: ConfigReader, client = None):
        self.client = client or pdr
        self.config = config_reader

    def fetch_data(self, symbol: str, column_name_remote: str, column_name: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        try:
            df = pd.read_csv(self.config.get_absolute_path('data/pmi.csv'))
            df['DATE'] = pd.to_datetime(df['DATE'])
            df['Actual'] = df['Actual'].fillna(df['Forecast'])
            df.set_index('DATE', inplace=True)
            df.sort_index(inplace=True)
            date_range_start = min(start_date, df.index.min())
            date_range_end = max(end_date, df.index.max())
            full_date_range = pd.date_range(start=date_range_start, end=date_range_end, freq='D')
            daily_df = df.reindex(full_date_range).interpolate(method='linear')
            daily_df = daily_df[(daily_df.index >= start_date) & (daily_df.index <= end_date)]
            daily_df.reset_index(inplace=True)
            daily_df.rename(columns={'index': 'DATE', 'Actual': 'PMI'}, inplace=True)
            daily_df = daily_df[['DATE', 'PMI']]
            daily_df.set_index('DATE', inplace=True)
            logging.info(f"{column_name} data has been fetched from OTHER.")
            return daily_df
        except Exception as e:
            logging.error(f"Error fetching {column_name} data from OTHER: {e}")
            return pd.DataFrame()
        