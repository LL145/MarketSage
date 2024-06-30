# src/main.py
import os
import logging
from datetime import datetime, time, timedelta
from data_fetcher import DataFetcher
from config_reader import ConfigReader
from data_processor import DataProcessor
import pytz

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.toml')
    global_config = ConfigReader(config_path)
    data_fetcher = DataFetcher(global_config)
    start = datetime(1995, 1, 1)
    end = get_ny_date_without_timezone()
    data_fetcher.fetch_all(start, end)
    data_processor = DataProcessor(global_config)
    data_processor.clean_and_fill_data()
    data_processor.process()

def get_ny_date_without_timezone():
    new_york_timezone = pytz.timezone('America/New_York')
    now_ny = datetime.now(new_york_timezone)
    target_date = now_ny if now_ny.time() >= time(16, 0) else now_ny - timedelta(days=1)
    target_date_without_tz = datetime.combine(target_date.date(), time(23, 0))
    while target_date_without_tz.weekday() >= 5:
        target_date_without_tz -= timedelta(days=1)
    return target_date_without_tz

if __name__ == "__main__":
  main()
