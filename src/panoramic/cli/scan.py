import itertools
import logging
import operator
import time

from typing import Any, Dict, Iterable

from panoramic.cli.metadata import MetadataClient


logger = logging.getLogger(__name__)

TERMINAL_STATES = {'COMPLETED', 'CANCELED', 'FAILED'}


def columns_to_tables(columns: Iterable[Dict]) -> Iterable[Dict]:
    """Map iterable of ordered column records to tables."""
    columns_grouped = itertools.groupby(columns, operator.itemgetter('table_schema', 'table_name'))
    return (
        {'name': table_name, 'schema': table_schema, 'columns': columns}
        for (table_schema, table_name), columns in columns_grouped
    )


class Scanner:

    """Scans metadata for a given source and scope."""

    source_id: str
    client: MetadataClient

    def __init__(self, source_id: str, client: MetadataClient = None):
        self.source_id = source_id

        if client is None:
            self.client = MetadataClient()

    def scan_tables(self, table_filter: str) -> Iterable[Dict]:
        """Scan tables for a given source and filter."""
        logger.debug(f'Starting get tables job with filter {table_filter}')
        job_id = self.client.create_get_tables_job()(self.source_id, table_filter)
        logger.debug(f'Get tables job with id {job_id} started with filter {table_filter}')

        self._wait_for_terminal_state(job_id)
        for table in self._collect_results(job_id):
            yield table

    def scan_columns(self, table_filter: str) -> Iterable[Dict]:
        """Scan columns for a given source and filter."""
        logger.debug('Starting get columns job')
        job_id = self.client.create_get_columns_job(self.source_id, table_filter)
        logger.debug(f'Get columns job with id {job_id} started with filter {table_filter}')

        self._wait_for_terminal_state(job_id)
        yield from self._collect_results(job_id)

    def _wait_for_terminal_state(self, job_id: str):
        """Wait for job to reach terminal state."""
        while True:
            logger.debug(f'Getting status for job with id {job_id}')
            status = self.client.get_job_status(job_id)
            logger.debug(f'Got status {status} for job with id {job_id}')
            if status in TERMINAL_STATES:
                return
            time.sleep(3)

    def _collect_results(self, job_id: str) -> Iterable[Dict[str, Any]]:
        """Collect all results for a given job."""
        offset = 0
        limit = 500

        while True:
            logger.debug(f'Fetching page number {offset // limit + 1} for job with id {job_id}')
            page = self.client.get_job_results(job_id, offset=offset, limit=limit)
            yield from page

            if len(page) < limit:
                # last page
                logger.debug(f'Finished fetcing all results for job with id {job_id}')
                return

            offset += limit
