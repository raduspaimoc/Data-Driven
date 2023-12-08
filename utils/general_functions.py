import json
import logging
from logging import FileHandler
from sqlalchemy import create_engine

LOGS_DIR = "logs/"
DEFAULT_LOG_FILE = 'logfile.log'


def get_logger(log_file: str = DEFAULT_LOG_FILE) -> logging.Logger:
    """
    Get a configured logger instance.

    Parameters:
    - log_file (str): The name of the log file.

    Returns:
    - logger (logging.Logger): Configured logger instance.
    """
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


def create_connection():
    """
    Create a SQLAlchemy engine for a database connection.

    Reads database configuration from 'utils/config.json'.

    Returns:
    - engine (sqlalchemy.engine.Engine): SQLAlchemy engine instance.
    """
    # Read configuration from JSON file
    with open('utils/config.json') as f:
        config = json.load(f)

    # Extract database connection details
    db_config = config['database']

    # Construct the connection string
    conn_str = f"mssql+pyodbc://{db_config['user']}:{db_config['password']}@{db_config['server']}/{db_config['database_name']}?driver={db_config['driver']}"

    # Add authentication details based on the configuration
    if not db_config['trusted_connection']:
        conn_str += f"&Trusted_Connection=No"

    # Construct the SQLAlchemy engine
    engine = create_engine(conn_str)

    # Create a connection
    # conn = engine.connect()
    return engine
