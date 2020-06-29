import logging

import click

from panoramic.cli.__version__ import __version__


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
def scan(source_id: str, table_filter: str):
    from panoramic.cli.scan import Scanner
    from panoramic.cli.refresh import Refresher
    from panoramic.cli.write import write

    scanner = Scanner(source_id)
    refresher = Refresher(source_id)
    for table in scanner.scan_tables_names(table_filter):
        table_name = f'{table["table_schema"]}.{table["table_name"]}'
        refresher.refresh_table(table_name)
        tables = scanner.scan_columns(table_filter=table_name)
        write(tables)
