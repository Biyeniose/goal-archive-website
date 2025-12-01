# app/logger.py
import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler

class AppLogger:
    """Application logger setup"""
    
    def __init__(self, name: str = "sports_api", log_to_file: bool = True):
        self.name = name
        self.log_to_file = log_to_file
        self.logger = self._setup_logger()
    
    def _setup_logger(self):
        """Setup logger with both file and console handlers"""
        
        # Create logger
        logger = logging.getLogger(self.name)
        logger.setLevel(logging.DEBUG)
        
        # Prevent duplicate handlers
        if logger.handlers:
            return logger

        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Console Handler (for development)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # File Handler (rotating files) - only if enabled
        if self.log_to_file:
            self._setup_file_handler(logger, formatter)
        
        return logger
    
    def _setup_file_handler(self, logger, formatter):
        """Setup rotating file handler"""
        # Create logs directory
        LOG_DIR = Path("logs")
        LOG_DIR.mkdir(exist_ok=True)
        
        file_handler = RotatingFileHandler(
            LOG_DIR / "dev_api.log",
            maxBytes=10*1024*1024,  # 10MB
            backupCount=6
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    def get_logger(self):
        """Get the configured logger instance"""
        return self.logger
    
    # Convenience methods
    def debug(self, message: str):
        self.logger.debug(message)
    
    def info(self, message: str):
        self.logger.info(message)
    
    def warning(self, message: str):
        self.logger.warning(message)
    
    def error(self, message: str):
        self.logger.error(message)
    
    def critical(self, message: str):
        self.logger.critical(message)

# Create a default instance
#app_logger = AppLogger()
