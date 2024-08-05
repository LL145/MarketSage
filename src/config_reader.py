# src/config_reader.py
import toml
import os
import pandas as pd
from typing import Dict
from datetime import datetime

class ConfigReader:
    def __init__(self, filename):
        self.config = toml.load(filename)
        self.base_dir = os.path.dirname(os.path.abspath(filename))

    def get(self, section, option, fallback=None):
        return self.config.get(section, {}).get(option, fallback)

    def get_section(self, section):
        return self.config.get(section, {})

    def get_absolute_path(self, relative_path):
        project_root = os.path.abspath(os.path.join(self.base_dir, '..'))
        return os.path.abspath(os.path.join(project_root, relative_path))
    
    def get_data_raw_absolute_path(self):
        return self.get_absolute_path(self.get('data', 'raw_data_file_name'))

    def get_data_filled_absolute_path(self):
        return self.get_absolute_path(self.get('data', 'filled_data_file_name'))
        
    def get_data_processed_absolute_path(self):
        return self.get_absolute_path(self.get('data', 'processed_data_file_name'))

    def get_chart_absolute_path(self):
        return self.get_absolute_path(self.get('data', 'chart_file_name'))

    def get_model_absolute_path(self):
        return self.get_absolute_path(self.get('data', 'model_file_name'))
    
    def get_data_config_map(self) -> Dict[str, Dict[str, str]]:
        df = pd.read_csv(self.get_absolute_path(self.get('data', 'data_config_file_name')))
        config_map = {}
        for _, row in df.iterrows():
            config_map[row['column_name']] = row.to_dict()
        return config_map

    def get_symbol_to_url_map(self) -> Dict[str, str]:
        df = pd.read_csv(self.get_absolute_path(self.get('data', 'data_config_file_name')))
        symbol_to_url = {}
        for _, row in df.iterrows():
            url = row['url']
            if isinstance(url, str) and url:
                symbol_to_url[row['column_name']] = row['url']
        return symbol_to_url

    def read_data(self, file_path) -> pd.DataFrame:
        abs_file_path = self.get_absolute_path(file_path)
        try:
            df = pd.read_csv(abs_file_path, parse_dates=['DATE'], index_col='DATE')
            return df
        except FileNotFoundError:
            raise FileNotFoundError(f"{abs_file_path} not found.")
        except Exception as e:
            raise e
    
    def read_data_raw(self) -> pd.DataFrame:
        file_path = self.get_data_raw_absolute_path()
        try:
            df = pd.read_csv(file_path, parse_dates=['DATE'], index_col='DATE')
            return df
        except FileNotFoundError:
            raise FileNotFoundError(f"{file_path} not found.")
        except Exception as e:
            raise e
    
    def read_data_filled(self) -> pd.DataFrame:
        file_path = self.get_data_filled_absolute_path()
        try:
            df = pd.read_csv(file_path, parse_dates=['DATE'], index_col='DATE')
            return df
        except FileNotFoundError:
            raise FileNotFoundError(f"{file_path} not found.")
        except Exception as e:
            raise e
    
    def read_data_processed(self) -> pd.DataFrame:
        file_path = self.get_data_processed_absolute_path()
        try:
            df = pd.read_csv(file_path, parse_dates=['DATE'], index_col='DATE')
            return df
        except FileNotFoundError:
            raise FileNotFoundError(f"{file_path} not found.")
        except Exception as e:
            raise e

    def save_data_raw(self, df: pd.DataFrame) -> None:
        file_path = self.get_data_raw_absolute_path()
        df.reset_index().rename(columns={'index': 'DATE'}).to_csv(file_path, index=False)
        print(f"Data has been saved to {file_path}")
    
    def save_data_filled(self, df: pd.DataFrame) -> None:
        file_path = self.get_data_filled_absolute_path()
        df.reset_index().rename(columns={'index': 'DATE'}).to_csv(file_path, index=False)
        print(f"Data has been saved to {file_path}")

    def save_data_processed(self, df: pd.DataFrame) -> None:
        file_path = self.get_data_processed_absolute_path()
        df.reset_index().rename(columns={'index': 'DATE'}).to_csv(file_path, index=False)
        print(f"Data has been saved to {file_path}")

    def save_data(self, df: pd.DataFrame, file_path) -> None:
        abs_path = self.get_absolute_path(file_path)
        df.reset_index().rename(columns={'index': 'DATE'}).to_csv(abs_path, index=False)
        print(f"Data has been saved to {abs_path}")

    def create_empty_file(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        df = pd.DataFrame(index=pd.date_range(start=start_date, end=end_date, freq='D'))
        return df