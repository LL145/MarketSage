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
            if symbol != 'VGTSX':
                data = self.client.download(symbol, start=start_date, end=end_date)
                data = data[[column_name_remote]].rename(columns={column_name_remote: column_name})
                data.index.rename('DATE', inplace=True)
                logging.info(f"{column_name} data has been fetched from Yahoo Finance.")
                return data
            else:
                data_vgtsx = self.client.download('VGTSX', start=start_date, end=end_date)
                data_vgtsx = data_vgtsx[[column_name_remote]].rename(columns={column_name_remote: column_name})
                data_vgtsx.index.rename('DATE', inplace=True)
                if end_date > datetime(2011, 2, 28):
                    data_vxus = self.client.download('VXUS', start=start_date, end=end_date)
                    data_vxus = data_vxus[[column_name_remote]].rename(columns={column_name_remote: column_name})
                    data_vxus.index.rename('DATE', inplace=True)
                    vgtsx_price_on_reference_date = data_vgtsx.loc['2011-02-28', column_name]
                    vxus_price_on_reference_date = data_vxus.loc['2011-02-28', column_name]
                    adjustment_ratio = vgtsx_price_on_reference_date / vxus_price_on_reference_date
                    data_vxus_adjusted = data_vxus.loc['2011-02-28':] * adjustment_ratio
                    data_combined = pd.concat([data_vgtsx.loc[:'2011-02-27'], data_vxus_adjusted])
                    logging.info(f"{column_name} data has been fetched from Yahoo Finance and adjusted for symbol VGTSX.")
                    return data_combined
                else: 
                    logging.info(f"{column_name} data has been fetched from Yahoo Finance for symbol VGTSX without adjustment.")
                    return data_vgtsx

        except Exception as e:
            logging.error(f"Error fetching {column_name} data from Yahoo Finance: {e}")
            return pd.DataFrame()