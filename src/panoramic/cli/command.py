import logging
from typing import Optional

import click

from panoramic.cli.context import get_company_name
from panoramic.cli.controller import reconcile
from panoramic.cli.executor import execute
from panoramic.cli.local import get_state as get_local_state
from panoramic.cli.local.file_utils import FilePackage
from panoramic.cli.local.writer import FileWriter
from panoramic.cli.parser import load_scanned_tables
from panoramic.cli.refresh import Refresher
from panoramic.cli.remote import get_state as get_remote_state
from panoramic.cli.scan import Scanner

logger = logging.getLogger(__name__)


def scan(source_id: str, filter: Optional[str]):
    """Scan all metdata for given source and filter."""
    # TODO: Obtain api version
    api_version = 'v1'
    scanner = Scanner(source_id)
    refresher = Refresher(source_id)
    writer = FileWriter(FilePackage.scanned)
    tables = scanner.scan_tables(table_filter=filter)
    with click.progressbar(list(tables)) as bar:
        for table in bar:
            # drop source name from schema
            sourceless_schema = table['table_schema'].split('.', 1)[1]
            table_name = f'{sourceless_schema}.{table["table_name"]}'
            try:
                refresher.refresh_table(table_name)
                raw_columns = scanner.scan_columns(table_filter=table_name)
                for table in load_scanned_tables(raw_columns, api_version):
                    writer.write_model(table)
            except Exception:
                print(f'Failed to scan table {table_name}')
                logger.debug(f'Failed to scan table {table_name}', exc_info=True)
                continue


def pull():
    """Pull models and data sources from remote."""
    company_name = get_company_name()
    remote_source = get_remote_state(company_name)
    local_source = get_local_state(company_name)

    actions = reconcile(local_source, remote_source)
    execute(actions)


def push():
    """Push models and data sources to remote."""
    company_name = get_company_name()
    remote_source = get_remote_state(company_name)
    local_source = get_local_state(company_name)

    actions = reconcile(remote_source, local_source)
    execute(actions)
