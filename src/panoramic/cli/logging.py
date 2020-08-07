import logging
import os
import sys
import traceback

from tqdm import tqdm


def echo_error(msg: str, exc_info: bool = False):
    tqdm.write(f'Error: {msg}')

    if exc_info:
        tqdm.write(traceback.format_exc())


def echo_info(msg: str):
    tqdm.write(msg)


def configure_logging():
    log_level = os.environ.get('LOG_LEVEL', 'ERROR').upper()
    logging.basicConfig(stream=sys.stdout, level=log_level)

    # Hide auth logs from output
    logging.getLogger('requests_oauthlib').setLevel(logging.INFO)


def log_error(logger: logging.Logger, message, exc: Exception):
    tqdm.write(f'Error: {message}')
    logger.debug(message, exc_info=exc)

