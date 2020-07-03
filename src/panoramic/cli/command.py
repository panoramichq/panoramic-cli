from typing import Optional

import click

from panoramic.cli.file_utils import (
    FileExtension,
    FilePackage,
    get_target_abs_filepath,
    write_yaml,
)
from panoramic.cli.layer import load_scanned_table, unload_scanned_table
from panoramic.cli.refresh import Refresher
from panoramic.cli.scan import Scanner


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
            raw_columns = scanner.scan_columns(table_filter=table_name)

            scanned_table = load_scanned_table(raw_columns)
            abs_filepath = get_target_abs_filepath(
                scanned_table.table_file_id, FileExtension.model_yaml, FilePackage.scanned
            )
            yaml_dict = unload_scanned_table(scanned_table)
            write_yaml(abs_filepath, yaml_dict)
