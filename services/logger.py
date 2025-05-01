import logging
from pathlib import Path
import os
from datetime import datetime

class Logger:
    def __init__(self, name, log_dir="logs"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        
        # Create logs directory if not exists
        self.log_dir = Path(log_dir)
        if not self.log_dir.exists():
            os.makedirs(self.log_dir)
        
        # Create log file with current date
        log_file = self.log_dir / f"{datetime.now().strftime('%Y-%m-%d')}.log"
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # Add file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        
        # Add console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
    
    def info(self, message):
        self.logger.info(message)
    
    def warning(self, message):
        self.logger.warning(message)
    
    def error(self, message):
        self.logger.error(message)
    
    def debug(self, message):
        self.logger.debug(message)
    
    def exception(self, message):
        self.logger.exception(message)