import logging
import os
from typing import Optional
from pathlib import Path

import click
import yaml

from panoramic.cli.__version__ import __version__


logging.basicConfig(level=logging.WARNING)


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
    from panoramic.cli.scan import Scanner, columns_to_tables
    from panoramic.cli.refresh import Refresher
    from panoramic.cli.write import write

    scanner = Scanner(source_id)
    refresher = Refresher(source_id)
    tables = scanner.scan_tables(filter)
    with click.progressbar(list(tables)) as bar:
        for table in bar:
            # drop source name from schema
            sourceless_schema = table['table_schema'].split('.', 1)[1]
            table_name = f'{sourceless_schema}.{table["table_name"]}'
            refresher.refresh_table(table_name)
            tables = columns_to_tables(scanner.scan_columns(table_filter=table_name))
            write(tables)


@cli.command(help='Configure pano CLI options')
def configure():
    client_id = click.prompt('Enter your client_id', type=str)
    client_secret = click.prompt('Enter your client_secret', hide_input=True, type=str)
    config_dir = Path.home() / '.pano'
    if not os.path.exists(config_dir):
        os.mkdir(config_dir)
    with open(config_dir / 'config', 'w+') as f:
        f.write(yaml.safe_dump({'client_id': client_id, 'client_secret': client_secret}))
