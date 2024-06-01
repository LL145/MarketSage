# src/data_sources/base_data_source.py
from abc import ABC, abstractmethod
from datetime import datetime
import pandas as pd

class BaseDataSource(ABC):

    @abstractmethod
    def fetch_data(self, symbol: str, column_name_remote: str, column_name: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        pass