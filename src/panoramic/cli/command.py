import logging
from typing import Optional

import click

from panoramic.cli.errors import UnexpectedTablesException
from panoramic.cli.file_utils import (
    FileExtension,
    FilePackage,
    get_target_abs_filepath,
    write_yaml,
)
from panoramic.cli.parser import load_scanned_tables
from panoramic.cli.refresh import Refresher
from panoramic.cli.scan import Scanner
from panoramic.cli.context import get_company_name
from panoramic.cli.controller import reconcile
from panoramic.cli.executor import execute
from panoramic.cli.state import get_local, get_remote

logger = logging.getLogger(__name__)


def scan(source_id: str, filter: Optional[str]):
    """Scan all metdata for given source and filter."""
    # TODO: Obtain api version
    api_version = 'v1'
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
                raw_columns = scanner.scan_columns(table_filter=table_name)

                scanned_tables = load_scanned_tables(raw_columns, api_version)
                if len(scanned_tables) > 1:
                    raise UnexpectedTablesException('Found unexpected table')
                scanned_table = scanned_tables[0]

                abs_filepath = get_target_abs_filepath(
                    scanned_table.table_file_name, FileExtension.model_yaml, FilePackage.scanned
                )
                write_yaml(abs_filepath, scanned_table.to_dict())
            except Exception:
                print(f'Failed to scan table {table_name}')
                logger.debug(f'Failed to scan table {table_name}', exc_info=True)
                continue


def pull():
    """Pull models and data sources from remote."""
    company_name = get_company_name()
    remote_source = get_remote(company_name)
    local_source = get_local(company_name)

    actions = reconcile(local_source, remote_source)
    execute(actions)


def push():
    """Push models and data sources to remote."""
    company_name = get_company_name()
    remote_source = get_remote(company_name)
    local_source = get_local(company_name)

    actions = reconcile(remote_source, local_source)
    execute(actions)
