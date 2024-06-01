# src/file_handler.py
import pandas as pd
from datetime import datetime

class FileHandler:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def read_file(self) -> pd.DataFrame:
        try:
            df = pd.read_csv(self.file_path, parse_dates=['DATE'], index_col='DATE')
            return df
        except FileNotFoundError:
            raise FileNotFoundError(f"{self.file_path} not found.")
        except Exception as e:
            raise e

    def save_file(self, df: pd.DataFrame) -> None:
        df.reset_index().rename(columns={'index': 'DATE'}).to_csv(self.file_path, index=False)
        print(f"Data has been saved to {self.file_path}")

    def create_empty_file(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        df = pd.DataFrame(index=pd.date_range(start=start_date, end=end_date, freq='D'))
        return df