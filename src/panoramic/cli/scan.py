import itertools
import logging
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import requests
from requests.exceptions import RequestException

from panoramic.cli.context import get_company_slug
from panoramic.cli.errors import (
    MissingFieldFileError,
    ScanException,
    SourceNotFoundException,
)
from panoramic.cli.field_mapper import map_column_to_field, map_error_to_field
from panoramic.cli.metadata import JobState, MetadataClient
from panoramic.cli.model_mapper import map_columns_to_model
from panoramic.cli.pano_model import PanoField, PanoModel

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


def _group_errors_by_column(
    errors: Sequence[MissingFieldFileError],
) -> Dict[Tuple[str, str], List[MissingFieldFileError]]:
    """Group errors by data_source+data_reference (table+column)."""
    errors_by_column: Dict[Tuple[str, str], List[MissingFieldFileError]] = defaultdict(list)
    for error in errors:
        errors_by_column[(error.data_source, error.data_reference)].append(error)
    return dict(errors_by_column)


def scan_fields_for_errors(errors: Sequence[MissingFieldFileError]) -> List[PanoField]:
    """Scan fields for missing file errors."""
    company_slug = get_company_slug()

    fields: List[PanoField] = []
    errors_by_column = _group_errors_by_column(errors)
    data_sources = {data_source for data_source, _ in errors_by_column}

    for data_source in data_sources:
        # TODO: Refresh before scan?
        connection, table_name = data_source.split('.', 1)
        for column in Scanner(company_slug, connection).scan_columns(table_filter=table_name):
            data_reference = column['data_reference']
            try:
                for error in errors_by_column[data_source, data_reference]:
                    # Create field for each dataset that is missing the field
                    fields.append(
                        map_column_to_field(
                            column,
                            slug=error.field_slug,
                            data_source=error.dataset_slug,
                            is_identifier=error.identifier,
                        )
                    )
                del errors_by_column[data_source, data_reference]
            except KeyError:
                pass  # Column does not map to missing field

    # errors for fields we were not able to scan
    fields.extend(map_error_to_field(error) for error in itertools.chain.from_iterable(errors_by_column.values()))

    return fields
