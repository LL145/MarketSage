# src/data_fetcher.py
import pandas as pd
from datetime import datetime
from typing import List
import logging
from data_sources.fred_data_source import FredDataSource
from data_sources.multpl_data_source import MultplDataSource
from data_sources.other_data_source import OtherDataSource
from data_sources.yfinance_data_source import YfinanceDataSource
from data_sources.bls_data_source import BlsDataSource
from config_reader import ConfigReader
from file_handler import FileHandler
import requests
from io import StringIO
import yfinance as yf
import numpy as np
from sklearn.linear_model import LinearRegression

logger = logging.getLogger(__name__)

class DataFetcher:
    def __init__(self, config: ConfigReader):
        self.config = config
        self.raw_data_file_name = self.config.get_absolute_path(self.config.get('data', 'raw_data_file_name'))
        self.data_config_file_name = self.config.get_absolute_path(self.config.get('data', 'data_config_file_name'))
        self.columns_info = self.config.get_data_config_map()
        self.file_handler = FileHandler(self.raw_data_file_name)
        self.data_sources = {
            'FRED': FredDataSource(config),
            'YFINANCE': YfinanceDataSource(),
            'BLS': BlsDataSource(),
            'MULTPL': MultplDataSource(config),
            'OTHER': OtherDataSource(config)
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
        self.post_process(start_date, end_date)
        logging.info("Post process done")

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
    
    def post_process(self, start_date: datetime, end_date: datetime):
        year = end_date.year
        month = end_date.month
        month_str = f"{month:02d}"
        day = end_date.day
        day_str = f"{day:02d}"
        df = self.config.read_data_raw()
        df1 = fetch_treasury_data(year, month)
        # 用 treasury.gov 的数据填充最近一天缺失的数据
        if pd.isna(df.loc[f"{year}{month_str}{day_str}", 'GS1']):
            df.loc[f"{year}{month_str}{day_str}", 'GS1'] = df1.loc[f"{year}{month_str}{day_str}", '1 Yr']
        if pd.isna(df.loc[f"{year}{month_str}{day_str}", 'GS2']):
            df.loc[f"{year}{month_str}{day_str}", 'GS2'] = df1.loc[f"{year}{month_str}{day_str}", '2 Yr']
        # 补充 RRPONTSYD 的缺失数据
        cutoff_date = pd.to_datetime('2013-09-30')
        df.loc[(df.index <= cutoff_date) & (df['RRPONTSYD'].isna()), 'RRPONTSYD'] = 0.001

        # WALCL
        fit_data = df.loc['2002-12-18':'2008-07-23', 'WALCL'].dropna()
        X = np.array((fit_data.index - fit_data.index[0]).days).reshape(-1, 1)
        y = fit_data.values
        model = LinearRegression()
        model.fit(X, y)
        missing_data_index = df.index < '2002-12-18'
        missing_X = np.array((df.loc[missing_data_index].index - fit_data.index[0]).days).reshape(-1, 1)
        df.loc[missing_data_index, 'WALCL'] = model.predict(missing_X)
        #===============
        self.file_handler.save_file(df)


def fetch_treasury_data(year, month):
    month_str = f"{month:02d}"
    url = f"https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/all/{year}{month_str}?type=daily_treasury_yield_curve&field_tdr_date_value_month={year}{month_str}&page&_format=csv"
    response = requests.get(url)
    csv_content = StringIO(response.content.decode('utf-8'))
    df = pd.read_csv(csv_content)
    df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y')
    df.set_index('Date', inplace=True)
    return df