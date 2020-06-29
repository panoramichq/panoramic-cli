import click

from panoramic.cli.refresh import Refresher
from panoramic.cli.scan import Scanner
from panoramic.cli.write import write


@click.group(context_settings={'help_option_names': ["-h", "--help"]})
def cli():
    pass


@cli.command(help='Scan models from source')
@click.argument('source-id', type=str, required=True)
@click.option('--filter', '-f', type=str, help='Filter down what schemas to scan')
def scan(source_id: str, table_filter: str):
    scanner = Scanner(source_id)
    refresher = Refresher(source_id)
    for table in scanner.scan_tables_names(table_filter):
        table_name = f'{table["table_schema"]}.{table["table_name"]}'
        refresher.refresh_table(table_name)
        tables = scanner.scan_columns(table_filter=table_name)
        write(tables)
