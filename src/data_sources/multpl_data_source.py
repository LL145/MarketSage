# src/data_sources/multpl_data_source.py
import requests
from bs4 import BeautifulSoup
from config_reader import ConfigReader
from data_sources.base_data_source import BaseDataSource
from datetime import datetime
import pandas as pd
import logging


class MultplDataSource(BaseDataSource):

    def __init__(self, config_reader: ConfigReader, client=None):
        self.client = client
        self.config_reader = config_reader
        self.symbol_to_url = config_reader.get_symbol_to_url_map()

    def fetch_data(self, symbol: str, column_name_remote: str, column_name: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        try:
            url = self.symbol_to_url.get(symbol)
            if not url:
                raise ValueError(f"URL for symbol {symbol} not found")
            response = requests.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            # Parse table data
            table = soup.find('table', {'id': 'datatable'})
            if table:
                rows = table.find_all('tr')
                data = []
                for row in rows[1:]:  # Skip header
                    cols = row.find_all('td')
                    if len(cols) == 2:
                        date = cols[0].text.strip()
                        value = cols[1].text.strip().replace('†', '').replace('%', '').strip()
                        data.append([date, value])

                df = pd.DataFrame(data, columns=['DATE', column_name])
                df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')
                df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
                df.set_index('DATE', inplace=True)
                df = df[(df.index >= start_date) & (df.index <= end_date)]
                df = df.loc[~df.index.duplicated(keep='first')]
                logging.info("Data retrieved successfully")
                logging.info(f"{column_name} data has been fetched from Multpl.")
                return df
            else:
                raise ValueError("Table not found")
        except requests.RequestException as e:
            logging.error(f"Network request error: {e}")
            raise
        except Exception as e:
            logging.error(f"Data processing error: {e}")
            raise
