import logging
import os

from pathlib import Path
from typing import Optional

import click
import yaml

from panoramic.cli.__version__ import __version__
from panoramic.cli.errors import (
    RefreshException,
    ScanException,
    SourceNotFoundException,
    TimeoutException,
)


@click.group(context_settings={'help_option_names': ["-h", "--help"]})
@click.option('--debug', is_flag=True, help='Enables debug mode')
@click.version_option(__version__)
def cli(debug):
    if debug:
        logger = logging.getLogger()
        logger.setLevel("DEBUG")

    from panoramic.cli.supported_version import is_version_supported

    if not is_version_supported(__version__):
        exit(1)


@cli.command(help='Scan models from source')
@click.argument('source-id', type=str, required=True)
@click.option('--filter', '-f', type=str, help='Filter down what schemas to scan')
def scan(source_id: str, filter: Optional[str]):
    from panoramic.cli.command import scan

    logger = logging.getLogger(__name__)
    try:
        scan(source_id, filter)
    except SourceNotFoundException as e:
        print(e)
        logger.debug('Source not found', exc_info=True)
    except (TimeoutException, ScanException, RefreshException):
        print('Internal error occured.')
        logger.debug('Internal error occured', exc_info=True)


@cli.command(help='Configure pano CLI options')
def configure():
    client_id = click.prompt('Enter your client_id', type=str)
    client_secret = click.prompt('Enter your client_secret', hide_input=True, type=str)
    config_dir = Path.home() / '.pano'
    if not os.path.exists(config_dir):
        os.mkdir(config_dir)
    with open(config_dir / 'config', 'w+') as f:
        f.write(yaml.safe_dump({'client_id': client_id, 'client_secret': client_secret}))
