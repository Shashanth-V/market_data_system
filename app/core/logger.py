import logging
import os
import sys
from logging.handlers import RotatingFileHandler

def setup_logger(name: str = "market_data_system") -> logging.Logger:
    """
    Configures and returns a production-ready logger.
    Supports concurrent console output and a rotating file log in the logs/ folder.
    """
    logger = logging.getLogger(name)
    
    # Avoid duplicate handlers if already configured
    if logger.hasHandlers():
        return logger
        
    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    logger.setLevel(log_level)
    
    # Formatter for structured logs
    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s [%(name)s:%(filename)s:%(lineno)d] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(log_level)
    logger.addHandler(console_handler)
    
    # File Handler (rotating logs to prevent running out of space)
    try:
        log_dir = "logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
            
        file_path = os.path.join(log_dir, "app.log")
        file_handler = RotatingFileHandler(
            file_path, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(log_level)
        logger.addHandler(file_handler)
    except Exception as e:
        # Fallback if logs directory cannot be created
        logger.warning(f"Could not create file log handler: {e}")
        
    return logger

# Singleton default logger
logger = setup_logger()
