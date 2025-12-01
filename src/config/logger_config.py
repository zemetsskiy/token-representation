import logging
import sys
from .config import Config

def setup_logging():
    log_level = getattr(logging, Config.LOG_LEVEL.upper(), logging.DEBUG)
    formatter = logging.Formatter(fmt='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    try:
        file_handler = logging.FileHandler('worker.log')
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    except Exception as e:
        logging.warning(f'Failed to create file handler: {e}')
    logging.getLogger('clickhouse_connect').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.info('Logging configured successfully')