import logging

from typing import Optional

import click

from panoramic.cli.refresh import Refresher
from panoramic.cli.scan import Scanner, columns_to_tables
from panoramic.cli.write import write


logger = logging.getLogger(__name__)


def scan(source_id: str, filter: Optional[str]):
    """Scan all metdata for given source and filter."""
    scanner = Scanner(source_id)
    refresher = Refresher(source_id)
    tables = scanner.scan_tables(filter)
    with click.progressbar(list(tables)) as bar:
        for table in bar:
            # drop source name from schema
            sourceless_schema = table['table_schema'].split('.', 1)[1]
            table_name = f'{sourceless_schema}.{table["table_name"]}'
            try:
                refresher.refresh_table(table_name)
                tables = columns_to_tables(scanner.scan_columns(table_filter=table_name))
                write(tables)
            except Exception:
                print(f'Failed to scan table {table_name}')
                logger.debug(f'Failed to scan table {table_name}', exc_info=True)
                continue
