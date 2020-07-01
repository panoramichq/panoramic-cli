import logging
import os

from pathlib import Path
from typing import Optional

import click
import yaml

from panoramic.cli.refresh import Refresher
from panoramic.cli.scan import Scanner, columns_to_tables
from panoramic.cli.write import write


logging.basicConfig(level=logging.WARNING)


@click.group(context_settings={'help_option_names': ["-h", "--help"]})
def cli():
    pass


@cli.command(help='Scan models from source')
@click.argument('source-id', type=str, required=True)
@click.option('--filter', '-f', type=str, help='Filter down what schemas to scan')
def scan(source_id: str, filter: Optional[str]):
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
