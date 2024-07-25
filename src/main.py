# src/main.py
import os
import logging
from datetime import datetime
from data_fetcher import DataFetcher
from config_reader import ConfigReader
from data_processor import DataProcessor
import util

from publisher import Publisher

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.toml')
    global_config = ConfigReader(config_path)
    global_config.get_data_config_map()
    data_fetcher = DataFetcher(global_config)
    start = datetime(1998, 1, 1)
    end = util.get_ny_date_without_timezone()
    
    data_fetcher.fetch_all(start, end)
    data_processor = DataProcessor(global_config)
    data_processor.clean_and_fill_data()
    data_processor.process()
    
    publisher = Publisher(global_config)
    publisher.predict()


if __name__ == "__main__":
  main()
