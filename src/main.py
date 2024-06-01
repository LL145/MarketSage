# src/main.py
import os
import logging
from data_fetcher import DataFetcher
from config_reader import ConfigReader
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.toml')
    global_config = ConfigReader(config_path)
    data_fetcher = DataFetcher(global_config)
    data_fetcher.fetch_all(datetime(1980, 1, 1), datetime.now())
    #data_fetcher.update_row(datetime(1979, 1, 1), datetime.now())
    #data_fetcher.update_column('US_POPULATION')

if __name__ == "__main__":
  main()
