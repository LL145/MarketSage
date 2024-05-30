# src/config_reader.py
import toml
import os
import pandas as pd
from typing import Dict

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
    
    def load_data_config(self, config_file: str) -> Dict[str, Dict[str, str]]:
        df = pd.read_csv(config_file)
        config_map = {}
        for _, row in df.iterrows():
            config_map[row['column_name']] = row.to_dict()
        return config_map

