import logging
from pathlib import Path
from typing import Optional

import click
import yaml
from dotenv import load_dotenv

from panoramic.cli.__version__ import __version__
from panoramic.cli.errors import SourceNotFoundException
from panoramic.cli.logging import log_error


@click.group(context_settings={'help_option_names': ["-h", "--help"]})
@click.option('--debug', is_flag=True, help='Enables debug mode')
@click.version_option(__version__)
def cli(debug):
    if debug:
        logger = logging.getLogger()
        logger.setLevel("DEBUG")

    load_dotenv()

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
    except SourceNotFoundException as source_exception:
        log_error(logger, 'Source not found', source_exception)
    except Exception as e:
        log_error(logger, 'Internal error occured.', e)


@cli.command(help='Pull models from remote')
def pull():
    from panoramic.cli.command import pull

    pull()


@cli.command(help='Push models to remote')
def push():
    from panoramic.cli.command import push

    push()


@cli.command(help='Configure pano CLI options')
def configure():
    client_id = click.prompt('Enter your client_id', type=str)
    client_secret = click.prompt('Enter your client_secret', hide_input=True, type=str)
    company_slug = click.prompt('Enter your company slug', type=str)

    config_dir = Path.home() / '.pano'
    if not config_dir.exists():
        config_dir.mkdir()

    with open(config_dir / 'config', 'w+') as f:
        f.write(yaml.safe_dump({'client_id': client_id, 'client_secret': client_secret}))

    context_file = Path.cwd() / 'pano.yaml'
    with open(context_file, 'w') as f:
        f.write(yaml.safe_dump({'company_slug': company_slug, 'api_version': 'v1'}))


@cli.command(help='List available data connections')
def list_connections():
    from panoramic.cli.command import list_connections

    logger = logging.getLogger(__name__)

    try:
        list_connections()
    except Exception as e:
        log_error(logger, 'Internal error occured.', e)
