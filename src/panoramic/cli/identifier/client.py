import logging
import time
from enum import Enum
from typing import Any, Dict, Optional
from urllib.parse import urljoin

from panoramic.auth import OAuth2Client
from panoramic.cli.config.auth import get_client_id, get_client_secret
from panoramic.cli.config.identifier import get_base_url
from panoramic.cli.errors import TimeoutException

logger = logging.getLogger(__name__)


class JobState(Enum):

    RUNNING = 'RUNNING'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'


TERMINAL_STATES = {JobState.COMPLETED, JobState.FAILED}


class IdentifierClient(OAuth2Client):

    """Identifier Parser HTTP API client."""

    base_url: str

    def __init__(
        self, base_url: Optional[str] = None, client_id: Optional[str] = None, client_secret: Optional[str] = None
    ):
        client_id = client_id if client_id is not None else get_client_id()
        client_secret = client_secret if client_secret is not None else get_client_secret()

        self.base_url = base_url if base_url is not None else get_base_url()

        super().__init__(client_id, client_secret)

    def create_identifier_job(self, company_slug: str, source_id: str, table_name: str) -> str:
        """Starts async "id parsing" job and return job id."""
        url = urljoin(self.base_url, f'{source_id}')
        params = {'company_slug': company_slug, 'table_name': table_name}
        logger.debug(f'Triggering a job for source {source_id} and table name: {table_name}')
        response = self.session.post(url, params=params, timeout=5)
        response.raise_for_status()
        return response.json()['data']['job_id']

    def get_job_status(self, job_id: str) -> JobState:
        """Get status of an async job."""
        url = urljoin(self.base_url, f'job/{job_id}')
        logger.debug(f'Getting job status for job {job_id}')
        response = self.session.get(url, timeout=5)
        response.raise_for_status()
        return JobState(response.json()['data']['status'])

    def get_job_results(self, job_id: str) -> Dict[str, Any]:
        """Get results of an async job."""
        url = urljoin(self.base_url, f'job/{job_id}')
        logger.debug(f'Getting job results for job {job_id}')
        response = self.session.get(url, timeout=5)
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
