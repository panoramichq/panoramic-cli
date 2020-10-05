import logging
import time
from enum import Enum
from typing import Any, Dict, Optional
from urllib.parse import urljoin

from panoramic.auth import OAuth2Client
from panoramic.cli.config.auth import get_client_id, get_client_secret
from panoramic.cli.config.join import get_base_url
from panoramic.cli.errors import TimeoutException

logger = logging.getLogger(__name__)


class JobState(Enum):

    RUNNING = 'RUNNING'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'


TERMINAL_STATES = {JobState.COMPLETED, JobState.FAILED}


class JoinClient(OAuth2Client):

    """Join Detection HTTP API client."""

    base_url: str

    def __init__(
        self, base_url: Optional[str] = None, client_id: Optional[str] = None, client_secret: Optional[str] = None
    ):
        client_id = client_id if client_id is not None else get_client_id()
        client_secret = client_secret if client_secret is not None else get_client_secret()

        self.base_url = base_url if base_url is not None else get_base_url()

        super().__init__(client_id, client_secret)

    def create_join_detection_job(self, company_slug: str, dataset_id: str) -> str:
        """Starts async "join detection" job and return job id."""
        url = urljoin(self.base_url, dataset_id)
        logger.debug(f'Triggering a join detection job for dataset {dataset_id}')
        response = self.session.post(url, params={'company_slug': company_slug}, timeout=5)
        response.raise_for_status()
        return response.json()['data']['job_id']

    def get_job_status(self, company_slug: str, job_id: str) -> JobState:
        """Get status of an async job."""
        url = urljoin(self.base_url, f'job/{job_id}')
        logger.debug(f'Getting job status for job {job_id} under company {company_slug}')
        response = self.session.get(url, params={'company_slug': company_slug}, timeout=5)
        response.raise_for_status()
        return JobState(response.json()['data']['status'])

    def get_job_results(self, company_slug: str, job_id: str) -> Dict[str, Any]:
        """Get results of an async job."""
        url = urljoin(self.base_url, f'job/{job_id}')
        logger.debug(f'Getting join detection job results for job {job_id} under company {company_slug}')
        response = self.session.get(url, params={'company_slug': company_slug}, timeout=5)
        response.raise_for_status()
        return response.json()['data']

    def wait_for_terminal_state(self, company_slug: str, job_id: str, timeout: int = 60) -> JobState:
        """Wait for job to reach terminal state."""
        tick_time = 1
        while True:
            logger.debug(f'Getting status for job with id {job_id} under company {company_slug}')
            status = self.get_job_status(company_slug, job_id)
            logger.debug(f'Got status {status} for job with id {job_id} under company {company_slug}')
            if status in TERMINAL_STATES:
                return status
            if timeout <= 0:
                raise TimeoutException(f'Timed out waiting for job {job_id} under company {company_slug} to complete')
            time.sleep(tick_time)
            timeout -= tick_time
