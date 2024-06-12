# src/main.py
import os
import logging
from datetime import datetime, timedelta
import pytz
from data_fetcher import DataFetcher
from config_reader import ConfigReader
from data_processor import DataProcessor
from trainer import Trainer



logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.toml')
    global_config = ConfigReader(config_path)
    data_fetcher = DataFetcher(global_config)


    start = datetime(1995, 1, 1)
    end = datetime.now()
    
    data_fetcher.fetch_all(start, end)
    data_processor = DataProcessor(global_config)
    data_processor.clean_and_fill_data()
    data_processor.process()

def get_new_york_datetime_naive():
    # 定义纽约时区
    new_york_tz = pytz.timezone('America/New_York')

    # 获取当前纽约时间
    new_york_time = datetime.now(new_york_tz)

    # 获取纽约时间的下午5点
    five_pm = new_york_time.replace(hour=17, minute=0, second=0, microsecond=0)

    # 判断当前时间是否超过下午5点
    if new_york_time > five_pm:
        result_datetime = new_york_time  # 今天的日期时间
    else:
        result_datetime = new_york_time - timedelta(days=1)  # 昨天的日期时间

    # 将带时区的datetime对象转换为不带时区的datetime对象
    naive_result_datetime = result_datetime.replace(tzinfo=None)

    return naive_result_datetime

if __name__ == "__main__":
  main()
