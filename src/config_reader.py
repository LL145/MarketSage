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

    def get_data_processed_absolute_path(self):
        return self.get_absolute_path(self.get('data', 'processed_data_file_name'))

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
    
    def read_data_raw(self) -> pd.DataFrame:
        try:
            df = pd.read_csv(self.get_data_raw_absolute_path(), parse_dates=['DATE'], index_col='DATE')
            return df
        except FileNotFoundError:
            raise FileNotFoundError(f"{self.file_path} not found.")
        except Exception as e:
            raise e

    def save_data_raw(self, df: pd.DataFrame) -> None:
        df.reset_index().rename(columns={'index': 'DATE'}).to_csv(self.get_data_raw_absolute_path(), index=False)
        print(f"Data has been saved to {self.file_path}")

    def create_empty_file(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        df = pd.DataFrame(index=pd.date_range(start=start_date, end=end_date, freq='D'))
        return df