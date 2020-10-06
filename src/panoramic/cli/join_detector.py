import logging
from typing import Any, Dict, List

import requests
from requests import RequestException

from panoramic.cli.errors import DatasetNotFoundException, JoinException
from panoramic.cli.join import JobState, JoinClient

logger = logging.getLogger(__name__)


class JoinDetector:
    def __init__(self, company_slug: str, client: JoinClient = None):
        self.company_slug = company_slug

        if client is None:
            client = JoinClient()

        self.client = client

    def fetch_token(self):
        self.client.fetch_token()

    def detect(self, dataset_id: str, timeout: int = 60) -> Dict[str, List[Dict[str, Any]]]:
        """Detect possible joins between models under a dataset."""
        logger.debug(f'Starting join detection job for dataset {dataset_id}')
        try:
            job_id = self.client.create_join_detection_job(self.company_slug, dataset_id)
            logger.debug(f'Join detection job with id {job_id} started for dataset {dataset_id}')
        except RequestException as e:
            if e.response is not None and e.response.status_code == requests.codes.not_found:
                raise DatasetNotFoundException(dataset_id).extract_request_id(e)
            raise JoinException(dataset_id).extract_request_id(e)

        try:
            state = self.client.wait_for_terminal_state(company_slug=self.company_slug, job_id=job_id, timeout=timeout)
            if state != JobState.COMPLETED:
                raise JoinException(dataset_id)

            logger.debug(f'Join detection job with id {job_id} completed for dataset {dataset_id}')
            return self.client.get_job_results(company_slug=self.company_slug, job_id=job_id)['joins']
        except RequestException as e:
            raise JoinException(dataset_id).extract_request_id(e)
