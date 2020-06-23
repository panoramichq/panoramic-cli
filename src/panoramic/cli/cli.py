from typing import Any, Dict

import click

from panoramic.cli.scan import Scanner
from panoramic.cli.write import write


@click.group(context_settings={'help_option_names': ["-h", "--help"]})
def cli():
    pass


@cli.command(help='Scan models from source')
@click.argument('source-id', type=str, required=True)
@click.option('--schema-filter', '-f', type=str, help='Filter down what schemas to scan')
def scan(source_id: str, schema_filter: str):
    tables = Scanner(source_id).run(schema_filter)
    write(tables)
