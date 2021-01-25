import logging
import os
import sys


def configure_logging():
    log_level = os.environ.get('LOG_LEVEL', 'ERROR').upper()
    logging.basicConfig(stream=sys.stdout, level=log_level)
