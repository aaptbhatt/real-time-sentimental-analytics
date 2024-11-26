import logging
from logging.handlers import RotatingFileHandler


def setup_logging(config, log_file_name=None):
    """Set up logging with file rotation and console output.

    Args:
        config (dict): Configuration dictionary.
        log_file_name (str): Optional log file name to override the default in config.
    """
    log_file = log_file_name if log_file_name else config['logging']['log_file']

    log_handler = RotatingFileHandler(
        log_file,
        maxBytes=config['logging']['log_file_size_mb'] * 1024 * 1024,
        backupCount=config['logging']['backup_count']
    )

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    log_handler.setFormatter(log_format)
    console_handler.setFormatter(log_format)

    logging.basicConfig(level=logging.INFO, handlers=[log_handler, console_handler])
    logging.info(f"Logging initialized, writing to: {log_file}")
