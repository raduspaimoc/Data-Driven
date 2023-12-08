import logging
from logging import FileHandler

LOGS_DIR = "logs/"
DEFAULT_LOG_FILE = 'logfile.log'


def get_logger(log_file: str = DEFAULT_LOG_FILE) -> logging.Logger:
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    # Create a console handler and set the level to INFO
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    # Create a file handler and set the level to INFO
    fh = FileHandler(LOGS_DIR + log_file)
    fh.setLevel(logging.INFO)

    # Create a formatter and set the formatter for the handlers
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger
