import logging

import requests
from requests.exceptions import RequestException

from panoramic.cli.errors import RefreshException, SourceNotFoundException
from panoramic.cli.metadata import JobState, MetadataClient

logger = logging.getLogger(__name__)


class Refresher:

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

    def refresh_table(self, table_name: str, timeout: int = 60):
        """Scan columns for a given source and scope."""
        logger.debug(f'Starting refresh metadata job for table {table_name}')
        try:
            job_id = self.client.create_refresh_job(self.company_slug, self.source_id, table_name)
            logger.debug(f'Refresh metadata job with id {job_id} started for table {table_name}')
        except RequestException as e:
            if e.response is not None and e.response.status_code == requests.codes.not_found:
                raise SourceNotFoundException(self.source_id).extract_request_id(e)
            raise RefreshException(self.source_id, table_name).extract_request_id(e)

        try:
            state = self.client.wait_for_terminal_state(job_id, timeout=timeout)
            if state != JobState.COMPLETED:
                raise RefreshException(self.source_id, table_name)

            logger.debug(f'Refresh metadata job with id {job_id} completed for table {table_name}')
        except RequestException as e:
            raise RefreshException(self.source_id, table_name).extract_request_id(e)
