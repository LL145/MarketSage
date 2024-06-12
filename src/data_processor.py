# src/data_processor.py
import pandas as pd
from datetime import datetime
import logging
from config_reader import ConfigReader

logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self, config: ConfigReader):
        self.config = config
        
    def clean_and_fill_data(self):
        df = self.config.read_data_raw()
        df = df.loc[~df.index.duplicated(keep='first')]
        df = df[df.index.weekday < 5]
        df = df.ffill()
        self.config.save_data_filled(df)
    
    def process_ma(self):
        df = self.config.read_data_filled()
        windows = {
            'MA7': 5,
            'MA1M': 20,
            'MA3M': 65,
            'MA6M': 130,
            'MA1Y': 260,
            'MA3Y': 782,
            'MA5Y': 1304
        }
        ma_columns = {}
        for column in df.columns:
            for key, window in windows.items():
                ma_column_name = f'{column}-{key}'
                ma_columns[ma_column_name] = df[column].rolling(window=window, min_periods=1).mean()
                ratio_column_name = f'{column}-{key}-R'
                ma_columns[ratio_column_name] = df[column] / ma_columns[ma_column_name]
        df = pd.concat([df, pd.DataFrame(ma_columns, index=df.index)], axis=1)
        # ===============额外特征
        df['US_GDP-PERCAPITA'] = df['US_GDP']/df['US_POPULATION']
        df['MONTH'] = df.index.month
        df['WEEKDAY'] = df.index.weekday + 1
        df['LEAPYEAR'] = (df.index.year % 4 == 0).astype(int)
        # =======================
        return df
    
    def process(self):
        data = self.process_ma()
        new_df = pd.DataFrame(index=data.index)

        # 直接复制
        base_columns = ['SP500_TR', 'SP500_VOLUME','FEDFUNDS', 'GS1', 'GS2', 'GS5', 
                        'GS10', 'GS30', 'DXY', 'VIX', 'US_UNEMPLOYMENT', 'SP500_PE',
                        'SHILLER_PE','SP500_DIVIDEND_YIELD', 'SP500_EARNINGS_YIELD',
                        'MONTH', 'WEEKDAY', 'LEAPYEAR', 'HIGH_YIELD_SPREAD']
        for base in base_columns:
            new_df[base] = data[base]

        # SP500每日涨幅
        new_df['SP500_TR_CHANGE'] = new_df['SP500_TR'].pct_change() * 100
        # M1, M2 /GDP
        new_df['M1_GDP_RATIO'] = data['US_M1']/data['US_GDP']
        new_df['M2_GDP_RATIO'] = data['US_M2']/data['US_GDP']

        # GDP,人口 增长率
        base_columns = ['US_GDP', 'US_POPULATION']
        suffixes = ['MA1Y-R', 'MA3Y-R', 'MA5Y-R']
        for base in base_columns:
            for suffix in suffixes:
                column_name = f'{base}-{suffix}'
                if column_name in data.columns:
                    new_df[column_name] = data[column_name]
        # ==========================

        base_columns = ['SP500_TR', 'SP500_VOLUME', 'RUSSELL2000', 'RUSSELL2000', 'VGTSX','GOLD', 'US_CPI', 'US_PCE', 'US_PPI','US_HOUSE']
        suffixes = ['MA7-R', 'MA1M-R','MA3M-R' ,'MA6M-R','MA1Y-R', 'MA3Y-R', 'MA5Y-R']
        for base in base_columns:
            for suffix in suffixes:
                column_name = f'{base}-{suffix}'
                if column_name in data.columns:
                    new_df[column_name] = data[column_name]
        
        # ==========================
        new_df = new_df.loc['2005-08-01':]
        
        self.config.save_data_processed(new_df)




        


