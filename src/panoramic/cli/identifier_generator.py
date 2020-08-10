import logging
from typing import List

import requests
from requests.exceptions import RequestException

from panoramic.cli.errors import IdentifierException, SourceNotFoundException
from panoramic.cli.identifier import IdentifierClient, JobState

logger = logging.getLogger(__name__)


class IdentifierGenerator:

    """Generate identifiers for a given source and table."""

    source_id: str
    client: IdentifierClient

    def __init__(self, company_slug: str, source_id: str, client: IdentifierClient = None):
        self.company_slug = company_slug
        self.source_id = source_id

        if client is None:
            client = IdentifierClient()

        self.client = client

    def fetch_token(self):
        self.client.fetch_token()

    def generate(self, table_name: str, timeout: int = 60) -> List[str]:
        """Generate identifiers for a given table."""
        logger.debug(f'Starting identifier generator job for table {table_name}')
        try:
            job_id = self.client.create_identifier_job(self.company_slug, self.source_id, table_name)
            logger.debug(f'Identifier generator job with id {job_id} started for table {table_name}')
        except RequestException as e:
            if e.response is not None and e.response.status_code == requests.codes.not_found:
                raise SourceNotFoundException(self.source_id)
            raise IdentifierException(self.source_id, table_name).extract_request_id(e)

        try:
            state = self.client.wait_for_terminal_state(job_id, timeout=timeout)
            if state != JobState.COMPLETED:
                raise IdentifierException(self.source_id, table_name)

            logger.debug(f'Identifier generator job with id {job_id} completed for table {table_name}')
            return self.client.get_job_results(job_id)['identifiers']
        except RequestException as e:
            raise IdentifierException(self.source_id, table_name).extract_request_id(e)
