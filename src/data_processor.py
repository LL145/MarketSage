# src/data_processor.py
import pandas as pd
from datetime import datetime
import logging
from config_reader import ConfigReader
from file_handler import FileHandler

logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self, file_name: str):
        self.file_handler = FileHandler(file_name)
        
    def process(self):

        
