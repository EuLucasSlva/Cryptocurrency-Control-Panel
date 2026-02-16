import logging
import sys
from datetime import datetime
from pathlib import Path


class ETLLogger:
    
    def __init__(self, name: str, log_to_file: bool = True):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        
        if self.logger.handlers:
            self.logger.handlers.clear()
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        if log_to_file:
            log_dir = Path(__file__).parent.parent / 'logs'
            log_dir.mkdir(exist_ok=True)
            
            log_file = log_dir / f"{name}_{datetime.now().strftime('%Y%m%d')}.log"
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
    
    def info(self, message: str):
        self.logger.info(message)
    
    def warning(self, message: str):
        self.logger.warning(message)
    
    def error(self, message: str, exc_info: bool = False):
        self.logger.error(message, exc_info=exc_info)
    
    def success(self, message: str):
        self.logger.info(f"SUCCESS: {message}")
    
    def debug(self, message: str):
        self.logger.debug(message)
