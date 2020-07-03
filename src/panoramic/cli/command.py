from typing import Optional

import click


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
