import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import click
from tqdm import tqdm

from panoramic.cli.context import get_company_slug
from panoramic.cli.controller import reconcile
from panoramic.cli.local import get_state as get_local_state
from panoramic.cli.local.executor import LocalExecutor
from panoramic.cli.local.file_utils import SystemDirectory
from panoramic.cli.local.writer import FileWriter
from panoramic.cli.logging import log_error
from panoramic.cli.parser import load_scanned_tables
from panoramic.cli.physical_data_source.client import PhysicalDataSourceClient
from panoramic.cli.refresh import Refresher
from panoramic.cli.remote import get_state as get_remote_state
from panoramic.cli.remote.executor import RemoteExecutor
from panoramic.cli.scan import Scanner

logger = logging.getLogger(__name__)


def list_connections():
    client = PhysicalDataSourceClient()

    sources = client.get_sources(get_company_slug())
    if len(sources) == 0:
        click.echo('No active connections set up. Use the Panoramic UI to create and configure data connections.')
    else:
        for source in client.get_sources(get_company_slug()):
            click.echo(source['source_name'])


def scan(source_id: str, filter: Optional[str], parallel: int = 1):
    """Scan all metadata for given source and filter."""
    company_slug = get_company_slug()
    scanner = Scanner(company_slug, source_id)
    refresher = Refresher(company_slug, source_id)
    writer = FileWriter()
    tables = scanner.scan_tables(table_filter=filter)
    executor = ThreadPoolExecutor(max_workers=parallel)

    tables = list(tables)
    progress_bar = tqdm(total=len(tables))

    def _process_table(table):
        # drop source name from schema
        sourceless_schema = table['table_schema'].split('.', 1)[1]
        table_name = f'{sourceless_schema}.{table["table_name"]}'

        try:
            refresher.refresh_table(table_name)
            raw_columns = scanner.scan_columns(table_filter=table_name)
            for table in load_scanned_tables(raw_columns):
                writer.write_model(table, package=SystemDirectory.SCANNED.value)
        except Exception as e:
            log_error(logger, f'Failed to scan table {table_name}', e)
        finally:
            progress_bar.update()

    for _ in executor.map(_process_table, tables):
        pass


def pull():
    """Pull models and data sources from remote."""
    company_slug = get_company_slug()
    remote_state = get_remote_state(company_slug)
    local_state = get_local_state()

    actions = reconcile(local_state, remote_state)
    executor = LocalExecutor()
    with tqdm(actions.actions) as bar:
        for action in bar:
            executor.execute(action)


def push():
    """Push models and data sources to remote."""
    company_slug = get_company_slug()
    remote_state = get_remote_state(company_slug)
    local_state = get_local_state()

    actions = reconcile(remote_state, local_state)
    executor = RemoteExecutor(company_slug)
    with tqdm(actions.actions) as bar:
        for action in bar:
            executor.execute(action)
