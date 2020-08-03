import logging
import os
import sys
import traceback

from requests.exceptions import RequestException
from tqdm import tqdm


def echo_error(msg: str, exc_info: bool = False):
    tqdm.write(f'Error: {msg}')

    if exc_info:
        tqdm.write(traceback.format_exc())


def echo_info(msg: str):
    tqdm.write(msg)


def configure_logging():
    log_level = os.environ.get('LOG_LEVEL', 'ERROR').upper()
    logging.basicConfig(stream=sys.stdout, level=log_level, format='%(levelname)s: %(message)s')


def log_error(logger: logging.Logger, message, exc: Exception):
    tqdm.write(f'Error: {message}')
    logger.debug(message, exc_info=exc)


def log_diesel_request_exception(logger: logging.Logger, exc: RequestException):
    logger.error(
        f'Request {exc.request.url} failed, debug ID: {exc.response.headers.get("x-diesel-request-id", "N/A")}'
    )
