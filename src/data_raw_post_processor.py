# src/data_raw_post_processor.py
import pandas as pd
import logging
from config_reader import ConfigReader
import numpy as np
import yfinance as yf
from datetime import datetime
import requests
from io import StringIO

logger = logging.getLogger(__name__)


class DataRawPostProcessor:

    def __init__(self, config: ConfigReader):
        self.config = config

    def process(self):
        df = self.config.read_data('data/data_raw.csv')
        df = self.other1(df)
        df = self.alt_data_source(df)
        df = self.shift(df)
        df = self.interpolate(df)
        df = self.extrapolate(df)
        df = self.other2(df)
        self.config.save_data(df, 'data/data_raw_processed.csv')

    def other1(self, df):
        cutoff_date = pd.Timestamp('2003-02-06')
        df.loc[df.index <= cutoff_date, 'RRPONTSYD'] = 0
        return df
    
    def other2(self, df):
        df['RRPGDP'] = 100 * df['RRPONTSYD']/df['US_GDP']
        df['RRPM2'] = 100 * df['RRPONTSYD']/df['US_M2']
        df['FEDASSETSGDP'] = df['WALCL']/df['US_GDP']
        
        cutoff_date = pd.Timestamp('2002-12-17')
        df.loc[df.index <= cutoff_date, 'FEDASSETSGDP'] = 66.0
        return df
        

    def alt_data_source(self, df):
        first_date = df.index[0]
        last_date = df.index[-1]
        first_date = pd.Timestamp(first_date).replace(hour=0,
                                                      minute=0,
                                                      second=0,
                                                      microsecond=0)
        last_date = pd.Timestamp(last_date).replace(hour=23,
                                                    minute=0,
                                                    second=0,
                                                    microsecond=0)
        for column in df.columns:
            if column == 'VGTSX':
                data_vgtsx = yf.download('VGTSX',
                                         start=first_date,
                                         end=last_date)
                data_vgtsx = data_vgtsx[[
                    'Adj Close'
                ]].rename(columns={'Adj Close': 'VGTSX'})
                data_vgtsx.index.rename('DATE', inplace=True)
                if last_date > datetime(2011, 2, 28):
                    data_vxus = yf.download('VXUS',
                                            start=first_date,
                                            end=last_date)
                    data_vxus = data_vxus[[
                        'Adj Close'
                    ]].rename(columns={'Adj Close': 'VGTSX'})
                    data_vxus.index.rename('DATE', inplace=True)
                    vgtsx_price_on_reference_date = data_vgtsx.loc[
                        '2011-02-28', 'VGTSX']
                    vxus_price_on_reference_date = data_vxus.loc['2011-02-28',
                                                                 'VGTSX']
                    adjustment_ratio = vgtsx_price_on_reference_date / vxus_price_on_reference_date
                    data_vxus_adjusted = data_vxus.loc[
                        '2011-02-28':] * adjustment_ratio
                    data_combined = pd.concat(
                        [data_vgtsx.loc[:'2011-02-27'], data_vxus_adjusted])
                    data_combined = data_combined.reindex(df.index)
                    df['VGTSX'] = data_combined['VGTSX']
                else:
                    pass
            elif column in ['GS1', 'GS2']:
                map = {'GS1': '1 Yr', 'GS2': '2 Yr'}
                year = last_date.year
                month = last_date.month
                month_str = f"{month:02d}"
                day = last_date.day
                day_str = f"{day:02d}"
                df1 = fetch_treasury_data(year, month)
                if pd.isna(df.loc[f"{year}{month_str}{day_str}", column]):
                    df.loc[f"{year}{month_str}{day_str}",
                           column] = df1.loc[f"{year}{month_str}{day_str}",
                                             map[column]]
        return df

    def shift(self, df):
        for column in df.columns:
            shift = self.config.get_data_config_map()[column]['date_shift']
            if not np.isnan(shift):
                shift = int(shift)
                new_index = df.index + pd.DateOffset(months=shift)
                temp_df = pd.DataFrame(data={column: df[column].values},
                                       index=new_index)
                temp_df = temp_df[~temp_df.index.duplicated(keep='first')]
                df[column] = temp_df.reindex(df.index)[column]
        return df

    def interpolate(self, df):
        interpolated_df = df.interpolate(method='linear', limit_area='inside')
        return interpolated_df

    def extrapolate(self, df):
        for column in df.columns:
            method = self.config.get_data_config_map()[column]['extrapolation']
            if method == 'linear':
                linear_extrapolate(df, column)
            elif method == 'ffill':
                df[column] = df[column].ffill()
        return df


def fetch_treasury_data(year, month):
    month_str = f"{month:02d}"
    url = f"https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/all/{year}{month_str}?type=daily_treasury_yield_curve&field_tdr_date_value_month={year}{month_str}&page&_format=csv"
    response = requests.get(url)
    csv_content = StringIO(response.content.decode('utf-8'))
    df = pd.read_csv(csv_content)
    df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y')
    df.set_index('Date', inplace=True)
    return df


def linear_extrapolate(df, column):
    if not np.isnan(df[column].iloc[-1]):
        return
    last_valid_index = df[column].last_valid_index()
    if last_valid_index is None:
        return
    i = df.index.get_loc(last_valid_index) + 1
    
    while i < len(df):
        current_date = df.index[i]
        previous_date = df.index[i - 1]
        previous_year_date = df.index[i - 366]
        df.loc[current_date, column] = df.loc[previous_date, column] + (df.loc[previous_date, column] - df.loc[previous_year_date, column]) / 365
        i += 1
