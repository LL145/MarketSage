# src/data_processor.py
import pandas as pd
import logging
from config_reader import ConfigReader
from scipy.interpolate import interp1d
import numpy as np

logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self, config: ConfigReader):
        self.config = config
        
    def clean_and_fill_data(self):
        df = self.config.read_data_raw()
        df = df.loc[~df.index.duplicated(keep='first')]
        for column in df.columns:
            freq = self.config.get_data_config_map()[column]['freq']
            if freq == 'D':
                df[column] = df[column].ffill()
            else:
                not_nan = df[column].notna()
                x = np.arange(len(df))
                x_known = x[not_nan]
                y_known = df[column][not_nan]
                f = interp1d(x_known, y_known, kind='linear', fill_value='extrapolate')
                df[column] = f(x)
        df = df[df.index.weekday < 5]
        self.config.save_data_filled(df)
    
    def process_ma(self):
        df = self.config.read_data_filled()
        windows = {
            'MA1M': 20,
            'MA3M': 65,
            'MA6M': 130,
            'MA1Y': 261,
            'MA3Y': 782,
            'MA5Y': 1304
        }
        ma_columns = {}
        for column in df.columns:
            for key, window in windows.items():
                ma_column_name = f'{column}-{key}'
                ma_columns[ma_column_name] = df[column].rolling(window=window, min_periods=1).mean()
                ratio_column_name = f'{column}-{key}-R'
                diff_column_name = f'{column}-{key}-D'
                ma_columns[ratio_column_name] = df[column] / ma_columns[ma_column_name]
                ma_columns[diff_column_name] = df[column] - ma_columns[ma_column_name]
        df = pd.concat([df, pd.DataFrame(ma_columns, index=df.index)], axis=1)
        
        # ===============额外特征
        df['US_GDP-PERCAPITA'] = df['US_GDP']/df['US_POPULATION']

        return df
    
    def process(self):
        data = self.process_ma()
        new_df = pd.DataFrame(index=data.index)

        # 直接复制
        base_columns = ['SP500_TR', 'FEDFUNDS', 'DGS3MO','GS1', 'GS2', 
                        'GS10', 'GS30', 'DXY', 'VIX', 'US_UNEMPLOYMENT', 'SP500_PE',
                        'HIGH_YIELD_SPREAD',
                        'T10Y3M','T10Y2Y', 'T10YIE','CONSUMER_SENTIMENT','RECESSION', 'NFCI']
        for base in base_columns:
            new_df[base] = data[base]

        # M1, M2, 基础货币 /GDP
        new_df['M1_GDP_RATIO'] = data['US_M1']/data['US_GDP']
        new_df['M2_GDP_RATIO'] = data['US_M2']/data['US_GDP']
        new_df['US_MONETARY_BASE_GDP_RATIO'] = data['US_MONETARY_BASE']/data['US_GDP']

        # 美联储总资产/GDP
        new_df['WALCL_GDP'] = data['WALCL'] / data['US_GDP']
        # 隔夜逆回购/GDP
        new_df['RRPONTSYD_GDP'] = data['RRPONTSYD'] / data['US_GDP']

        # ==========================
        base_columns = ['SP500_TR', 'RUSSELL2000', 'NASDAQ', 'VGTSX',
                        'US_CPI', 'US_PCE', 'US_PPI', 
                        'DXY',
                        'RRPONTSYD', 'WALCL','US_GDP', 'GDPC1',
                        'GS2', 'GS30',
                        'OIL', 'GOLD',
                        'US_POPULATION', 'US_MONETARY_BASE', 'US_M1', 'US_M2', 
                        'US_HOUSE', 'US_UNEMPLOYMENT', 
                        'FEDFUNDS','PAYEMS',
                        'HIGH_YIELD_SPREAD']
        suffixes = ['MA1M-R','MA3M-R' ,'MA6M-R','MA1Y-R', 'MA3Y-R', 'MA5Y-R']
        for base in base_columns:
            for suffix in suffixes:
                column_name = f'{base}-{suffix}'
                if column_name in data.columns:
                    new_df[column_name] = data[column_name]

        # =========================
        base_columns = ['T10Y3M', 'T10YIE', 'FEDFUNDS', 'GS1', 'GS2', 'GS10', 'GS30',
                        'HIGH_YIELD_SPREAD', 'US_UNEMPLOYMENT',
                        'SP500_PE']
        suffixes = ['MA3M-D' ,'MA6M-D','MA1Y-D', 'MA3Y-D', 'MA5Y-D']
        for base in base_columns:
            for suffix in suffixes:
                column_name = f'{base}-{suffix}'
                if column_name in data.columns:
                    new_df[column_name] = data[column_name]
        # ==========================
        new_df = new_df.loc['2005-08-01':]
        
        self.config.save_data_processed(new_df)
