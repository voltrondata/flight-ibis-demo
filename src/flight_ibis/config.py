import os
import sys
import logging
from pathlib import Path

# Constants
SCRIPT_DIR = Path(__file__).parent.resolve()
LOG_DIR = Path("logs").resolve()
DATA_DIR = Path("data").resolve()
DUCKDB_DB_FILE = DATA_DIR / "tpch.duckdb"
DUCKDB_THREADS = 4
DUCKDB_MEMORY_LIMIT = "4GB"
TIMER_TEXT = "{name}: Elapsed time: {:.4f} seconds"

# Logging Constants
LOGGING_FORMAT = '%(asctime)s - %(levelname)-8s %(message)s'
LOGGING_DATEFMT = '%Y-%m-%d %H:%M:%S %Z'
LOGGING_LEVEL = getattr(logging, os.getenv("LOG_LEVEL", "INFO"))
BASIC_LOGGING_KWARGS = dict(format=LOGGING_FORMAT,
                            datefmt=LOGGING_DATEFMT,
                            level=LOGGING_LEVEL
                            )
STDOUT_LOGGING_KWARGS = dict(stream=sys.stdout)


def get_logger(filename: str = None,
               filemode: str = "a",
               logger_name: str = None,
               log_level: int = LOGGING_LEVEL
               ):
    logger = logging.getLogger(name=logger_name)
    logger.setLevel(log_level)

    # Create a formatter for the log messages
    formatter = logging.Formatter(fmt=LOGGING_FORMAT)

    # Create a stream handler to log to stdout
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level=log_level)
    console_handler.setFormatter(fmt=formatter)

    logger.addHandler(hdlr=console_handler)

    # Create a file handler to log to a file
    if filename:
        file_handler = logging.FileHandler(filename=LOG_DIR / filename,
                                           mode=filemode)
        file_handler.setLevel(level=log_level)
        file_handler.setFormatter(fmt=formatter)
        logger.addHandler(hdlr=file_handler)

    return logger
