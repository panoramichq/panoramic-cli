import logging
import os
import sys

from tqdm import tqdm


def configure_logging():
    log_level = os.environ.get('LOG_LEVEL', 'ERROR').upper()
    logging.basicConfig(stream=sys.stdout, level=log_level, format='%(levelname)s: %(message)s')


def log_error(logger: logging.Logger, message, exc: Exception):
    if logger.level == logging.DEBUG:
        # This means we are in a debug mode
        logger.debug(message, exc_info=exc)
    else:
        tqdm.write(f'ERROR: {message}')
