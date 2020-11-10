import logging
from typing import Dict, Iterable, Optional

import requests
from requests.exceptions import RequestException

from panoramic.cli.errors import ScanException, SourceNotFoundException
from panoramic.cli.metadata import JobState, MetadataClient
from panoramic.cli.model_mapper import map_columns_to_model
from panoramic.cli.pano_model import PanoModel

logger = logging.getLogger(__name__)


class Scanner:

    """Scans metadata for a given source and scope."""

    source_id: str
    client: MetadataClient

    def __init__(self, company_slug: str, source_id: str, client: MetadataClient = None):
        self.company_slug = company_slug
        self.source_id = source_id

        if client is None:
            self.client = MetadataClient()

    def fetch_token(self):
        self.client.fetch_token()

    def scan_tables(self, *, table_filter: Optional[str] = None, timeout: int = 60) -> Iterable[Dict]:
        """Scan tables for a given source and filter."""
        logger.debug(f'Starting get tables job with filter {table_filter}')
        try:
            job_id = self.client.create_get_tables_job(self.company_slug, self.source_id, table_filter)
            logger.debug(f'Get tables job with id {job_id} started with filter {table_filter}')
        except RequestException as e:
            if e.response is not None and e.response.status_code == requests.codes.not_found:
                raise SourceNotFoundException(self.source_id).extract_request_id(e)
            raise ScanException(self.source_id, table_filter).extract_request_id(e)

        try:
            state = self.client.wait_for_terminal_state(job_id, timeout=timeout)
            if state != JobState.COMPLETED:
                raise ScanException(self.source_id, table_filter)

            logger.debug(f'Get tables job with id {job_id} completed with filter {table_filter}')
            yield from self.client.collect_results(job_id)
        except RequestException as e:
            raise ScanException(self.source_id, table_filter).extract_request_id(e)

    def scan_columns(self, *, table_filter: Optional[str] = None, timeout: int = 60) -> Iterable[Dict]:
        """Scan columns for a given source and filter."""
        logger.debug('Starting get columns job')
        try:
            job_id = self.client.create_get_columns_job(self.company_slug, self.source_id, table_filter)
            logger.debug(f'Get columns job with id {job_id} started with filter {table_filter}')
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == requests.codes.not_found:
                raise SourceNotFoundException(self.source_id).extract_request_id(e)
            raise ScanException(self.source_id, table_filter).extract_request_id(e)

        try:
            state = self.client.wait_for_terminal_state(job_id, timeout=timeout)
            if state != JobState.COMPLETED:
                raise ScanException(self.source_id, table_filter)

            logger.debug(f'Get columns job with id {job_id} completed with filter {table_filter}')
            yield from self.client.collect_results(job_id)
        except RequestException as e:
            raise ScanException(self.source_id, table_filter).extract_request_id(e)

    def scan_columns_grouped(self, *, table_filter: Optional[str] = None, timeout: int = 60) -> Iterable[PanoModel]:
        """Scan columns for a given source and group them by model and data source."""
        yield from map_columns_to_model(self.scan_columns(table_filter=table_filter, timeout=timeout))
