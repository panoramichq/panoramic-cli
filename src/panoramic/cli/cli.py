import logging
import sys
from typing import Optional

import click
from dotenv import load_dotenv

from panoramic.cli.__version__ import __version__
from panoramic.cli.context import ContextAwareCommand
from panoramic.cli.local.file_utils import Paths
from panoramic.cli.logging import echo_error


@click.group(context_settings={'help_option_names': ["-h", "--help"]})
@click.option('--debug', is_flag=True, help='Enables debug mode')
@click.version_option(__version__)
def cli(debug):
    if debug:
        logger = logging.getLogger()
        logger.setLevel("DEBUG")

    load_dotenv(dotenv_path=Paths.dotenv_file())

    from panoramic.cli.supported_version import is_version_supported

    if not is_version_supported(__version__):
        exit(1)


@cli.command(help='Scan models from source', cls=ContextAwareCommand)
@click.argument('source-id', type=str, required=True)
@click.option('--filter', '-f', type=str, help='Filter down what schemas to scan')
@click.option('--parallel', '-p', type=int, default=8, help='Parallelize metadata scan')
def scan(source_id: str, filter: Optional[str], parallel: int):
    from panoramic.cli.command import scan as scan_command

    try:
        scan_command(source_id, filter, parallel)
    except Exception:
        echo_error('Internal error occurred', exc_info=True)
        sys.exit(1)


@cli.command(help='Pull models from remote', cls=ContextAwareCommand)
def pull():
    from panoramic.cli.command import pull

    try:
        pull()
    except Exception:
        echo_error('Internal error occurred', exc_info=True)
        sys.exit(1)


@cli.command(help='Push models to remote', cls=ContextAwareCommand)
def push():
    from panoramic.cli.command import push

    try:
        push()
    except Exception:
        echo_error('Internal error occurred', exc_info=True)
        sys.exit(1)


@cli.command(help='Configure pano CLI options')
def configure():
    from panoramic.cli.command import configure

    try:
        configure()
    except Exception:
        echo_error('Internal error occurred', exc_info=True)
        sys.exit(1)


@cli.command(help='Initialize metadata repository')
def init():
    from panoramic.cli.command import initialize

    try:
        initialize()
    except Exception:
        echo_error('Internal error occurred', exc_info=True)
        sys.exit(1)


@cli.command(help='List available data connections', cls=ContextAwareCommand)
def list_connections():
    from panoramic.cli.command import list_connections

    try:
        list_connections()
    except Exception:
        echo_error('Internal error occurred', exc_info=True)
        sys.exit(1)


@cli.command(help='List available data connections')
def list_companies():
    try:
        list_companies()
    except Exception:
        echo_error('Internal error occurred', exc_info=True)
        sys.exit(1)
