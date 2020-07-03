from typing import Optional

import click

from panoramic.cli.file_utils import (
    FileExtension,
    FilePackage,
    get_target_abs_filepath,
    write_yaml,
)


def scan(source_id: str, filter: Optional[str]):
    from panoramic.cli.scan import Scanner
    from panoramic.cli.refresh import Refresher
    from panoramic.cli.parser import load_scanned_table

    scanner = Scanner(source_id)
    refresher = Refresher(source_id)
    tables = scanner.scan_tables(filter)
    with click.progressbar(list(tables)) as bar:
        for table in bar:
            # drop source name from schema
            sourceless_schema = table['table_schema'].split('.', 1)[1]
            table_name = f'{sourceless_schema}.{table["table_name"]}'
            refresher.refresh_table(table_name)
            raw_columns = scanner.scan_columns(table_filter=table_name)

            scanned_table = load_scanned_table(raw_columns)
            abs_filepath = get_target_abs_filepath(
                scanned_table.table_file_id, FileExtension.model_yaml, FilePackage.scanned
            )
            write_yaml(abs_filepath, scanned_table.to_dict())
