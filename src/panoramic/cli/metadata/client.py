import logging
import time
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urljoin

from panoramic.auth import OAuth2Client
from panoramic.cli.clients import VersionedClient
from panoramic.cli.config.auth import get_client_id, get_client_secret
from panoramic.cli.config.metadata import get_base_url
from panoramic.cli.errors import TimeoutException

logger = logging.getLogger(__name__)


class JobState(Enum):

    NOT_SUBMITTED = 'NOT_SUBMITTED'
    STARTING = 'STARTING'
    RUNNING = 'RUNNING'
    COMPLETED = 'COMPLETED'
    CANCELED = 'CANCELED'
    FAILED = 'FAILED'
    CANCELLATION_REQUESTED = 'CANCELLATION_REQUESTED'
    ENQUEUED = 'ENQUEUED'
    PLANNING = 'PLANNING'
    PENDING = 'PENDING'
    METADATA_RETRIEVAL = 'METADATA_RETRIEVAL'
    QUEUED = 'QUEUED'
    ENGINE_START = 'ENGINE_START'
    EXECUTION_PLANNING = 'EXECUTION_PLANNING'
    INVALID_STATE = 'INVALID_STATE'


TERMINAL_STATES = {JobState.COMPLETED, JobState.CANCELED, JobState.FAILED}


class MetadataClient(OAuth2Client, VersionedClient):

    """Metadata HTTP API client."""

    base_url: str

    def __init__(
        self,
        base_url: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
    ):
        base_url = base_url if base_url is not None else get_base_url()
        client_id = client_id if client_id is not None else get_client_id()
        client_secret = client_secret if client_secret is not None else get_client_secret()

        super().__init__(client_id, client_secret)
        self.base_url = base_url

    def create_refresh_job(self, company_slug: str, source_id: str, table_name: str):
        """Starts async "refresh metadata" job and return job id."""
        url = urljoin(self.base_url, f'{source_id}/refresh')
        params = {'table_name': table_name, 'company_slug': company_slug}
        logger.debug(f'Refreshing table for source {source_id} and name: {table_name}')
        response = self.session.post(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()['data']['job_id']

    def create_get_tables_job(self, company_slug: str, source_id: str, table_filter: Optional[str]) -> str:
        """Starts async "get tables" job and return job id."""
        url = urljoin(self.base_url, f'{source_id}/tables')
        params = (
            {'company_slug': company_slug}
            if table_filter is None
            else {'company_slug': company_slug, 'table_filter': table_filter}
        )
        logger.debug(f'Requesting tables for source {source_id} and filter: {table_filter}')
        response = self.session.post(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()['data']['job_id']

    def create_get_columns_job(self, company_slug: str, source_id: str, table_filter: Optional[str]) -> str:
        """Starts async "get columns" job and return job id."""
        url = urljoin(self.base_url, f'{source_id}/columns')
        params = (
            {'company_slug': company_slug}
            if table_filter is None
            else {'company_slug': company_slug, 'table_filter': table_filter}
        )
        logger.debug(f'Requesting columns for source {source_id} and filter: {table_filter}')
        response = self.session.post(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()['data']['job_id']

    def get_job_status(self, job_id: str) -> JobState:
        """Get status of an async job."""
        url = urljoin(self.base_url, f'job/{job_id}')
        logger.debug(f'Getting job status for job {job_id}')
        response = self.session.get(url, timeout=30)
        response.raise_for_status()
        return JobState(response.json()['data']['job_status'])

    def get_job_results(self, job_id: str, offset: int = 0, limit: int = 500) -> List[Dict[str, Any]]:
        """Get results of an async job."""
        url = urljoin(self.base_url, f'job/{job_id}/results')
        params = {'offset': offset, 'limit': limit}
        logger.debug(f'Getting job results for job {job_id}')
        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()['data']

    def wait_for_terminal_state(self, job_id: str, timeout: int = 60) -> JobState:
        """Wait for job to reach terminal state."""
        tick_time = 1
        while True:
            logger.debug(f'Getting status for job with id {job_id}')
            status = self.get_job_status(job_id)
            logger.debug(f'Got status {status} for job with id {job_id}')
            if status in TERMINAL_STATES:
                return status
            if timeout <= 0:
                raise TimeoutException(f'Timed out waiting for job {job_id} to complete')
            time.sleep(tick_time)
            timeout -= tick_time

    def collect_results(self, job_id: str, limit: int = 500) -> Iterable[Dict[str, Any]]:
        """Collect all results for a given job."""
        offset = 0

        while True:
            logger.debug(f'Fetching page number {offset // limit + 1} for job with id {job_id}')
            page = self.get_job_results(job_id, offset=offset, limit=limit)
            yield from page

            if len(page) < limit:
                # last page
                logger.debug(f'Finished fetching all results for job with id {job_id}')
                return

            offset += limit
