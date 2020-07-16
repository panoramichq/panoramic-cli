import logging
import os
import sys

from tqdm import tqdm


def configure_logging():
    log_level = os.environ.get('LOG_LEVEL', 'ERROR').upper()
    logging.basicConfig(stream=sys.stdout, level=log_level, format='%(levelname)s: %(message)s')


def log_error(logger: logging.Logger, message, exc: Exception):
    tqdm.write(f'ERROR: {message}')
    logger.debug(message, exc_info=exc)
