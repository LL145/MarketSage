# src/data_fetcher.py
import pandas as pd
from datetime import datetime
from typing import List
import logging
from data_sources.fred_data_source import FredDataSource
from data_sources.multpl_data_source import MultplDataSource
from data_sources.yfinance_data_source import YfinanceDataSource
from config_reader import ConfigReader
from file_handler import FileHandler

logger = logging.getLogger(__name__)

class DataFetcher:
    def __init__(self, config: ConfigReader):
        self.config = config
        self.raw_data_file_name = self.config.get_absolute_path(self.config.get('data', 'raw_data_file_name'))
        self.data_config_file_name = self.config.get_absolute_path(self.config.get('data', 'data_config_file_name'))
        self.columns_info = self.config.get_data_config_map()
        self.file_handler = FileHandler(self.raw_data_file_name)
        self.data_sources = {
            'FRED': FredDataSource(),
            'YFINANCE': YfinanceDataSource(),
            'MULTPL': MultplDataSource(config)
        }

    def fetch_data(self, start_date: datetime, end_date: datetime, column_names: List[str] = None) -> pd.DataFrame:
        df = pd.DataFrame(index=pd.date_range(start=start_date, end=end_date, freq='D'))

        if column_names is None:
            column_names = list(self.columns_info.keys())

        for column_name in column_names:
            info = self.columns_info.get(column_name)
            if not info:
                logging.error(f"Column {column_name} not found in the configuration.")
                continue

            column_name_remote = info['column_name_remote']
            symbol = info['symbol']
            source = info['source']

            data_source = self.data_sources.get(source)
            if data_source:
                new_data = data_source.fetch_data(symbol, column_name_remote, column_name, start_date, end_date)
                if not new_data.empty:
                    df = df.join(new_data, how='left')
                else:
                    logging.warning(f"No data fetched for {column_name} from {source}")
            else:
                logging.error(f"Unknown data source: {source}")

        return df

    def fetch_all(self, start_date: datetime, end_date: datetime) -> None:
        logging.info(f"Fetching all data from {start_date} to {end_date}")
        df = self.fetch_data(start_date, end_date)
        self.file_handler.save_file(df)
        logging.info(f"All data has been saved to {self.raw_data_file_name}")

    def update_column(self, column_name: str) -> None:
        logging.info(f"Updating column {column_name}")
        try:
            df = self.file_handler.read_file()
        except FileNotFoundError:
            logging.error(f"{self.raw_data_file_name} not found.")
            return

        start_date = df.index.min()
        end_date = df.index.max()

        if column_name in df.columns:
            logging.info(f"{column_name} column already exists in the file.")
            return

        new_data = self.fetch_data(start_date, end_date, [column_name])
        if not new_data.empty:
            df = df.join(new_data, how='left')
            self.file_handler.save_file(df)
            logging.info(f"Column {column_name} has been updated in {self.raw_data_file_name}")
        else:
            logging.warning(f"No data fetched for {column_name}.")

    def update_row(self, start_date: datetime, end_date: datetime) -> None:
        logging.info(f"Updating rows from {start_date} to {end_date}")
        try:
            df = self.file_handler.read_file()
        except FileNotFoundError:
            logging.error(f"{self.raw_data_file_name} not found.")
            return

        existing_columns = df.columns.tolist()
        new_data = self.fetch_data(start_date, end_date, existing_columns)
        if not new_data.empty:
            df = df.combine_first(new_data)
            self.file_handler.save_file(df)
            logging.info(f"Rows from {start_date} to {end_date} have been updated in {self.raw_data_file_name}")
        else:
            logging.warning("No new data fetched for the given date range.")