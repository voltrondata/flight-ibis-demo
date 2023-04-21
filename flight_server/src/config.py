import os
import sys
import logging
from pathlib import Path

# Constants
SCRIPT_DIR = Path(__file__).parent.resolve()
LOG_DIR = SCRIPT_DIR / "logs"
DATA_DIR = SCRIPT_DIR / "data"
DUCKDB_DB_FILE = DATA_DIR / "tpch.duckdb"

# Logging Constants
LOGGING_FORMAT = '%(asctime)s - %(levelname)-8s %(message)s'
LOGGING_DATEFMT = '%Y-%m-%d %H:%M:%S %Z'
LOGGING_LEVEL = getattr(logging, os.getenv("LOG_LEVEL", "INFO"))
BASIC_LOGGING_KWARGS = dict(format=LOGGING_FORMAT,
                            datefmt=LOGGING_DATEFMT,
                            level=LOGGING_LEVEL
                            )
FILE_LOGGING_KWARGS = dict(filename=LOG_DIR / "flight_server.log",
                           filemode="w"
                           )
STDOUT_LOGGING_KWARGS = dict(stream=sys.stdout)


def get_stdout_logger():
    logging.basicConfig(**BASIC_LOGGING_KWARGS,
                        **STDOUT_LOGGING_KWARGS
                        )

    return logging.getLogger()


def get_file_logger():
    logging.basicConfig(**BASIC_LOGGING_KWARGS,
                        **FILE_LOGGING_KWARGS
                        )

    return logging.getLogger()
