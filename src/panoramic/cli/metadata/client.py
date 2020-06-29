from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from panoramic.auth import OAuth2Client
from panoramic.cli.config.auth import get_client_id, get_client_secret
from panoramic.cli.config.metadata import get_base_url


class MetadataClient(OAuth2Client):

    """Metadata HTTP API client."""

    def __init__(
        self, base_url: Optional[str] = None, client_id: Optional[str] = None, client_secret: Optional[str] = None
    ):
        base_url = base_url if base_url is not None else get_base_url()
        client_id = client_id if client_id is not None else get_client_id()
        client_secret = client_secret if client_secret is not None else get_client_secret()

        super().__init__(client_id, client_secret)
        self.base_url = base_url

    def create_refresh_job(self, source_id: str, table_name: str):
        """Starts async "refresh metadata" job and return job id."""
        url = urljoin(self.base_url, f'{source_id}/refresh?table-name={table_name}')
        params = {'table-filter': table_name}
        response = self.session.post(url, params=params)
        response.raise_for_status()
        return response.json()['job_id']

    def create_get_tables_job(self, source_id: str, table_filter: str) -> str:
        """Starts async "get tables" job and return job id."""
        url = urljoin(self.base_url, f'{source_id}/tables')
        params = {'table-filter': table_filter}
        response = self.session.post(url, params=params)
        response.raise_for_status()
        return response.json()['job_id']

    def create_get_columns_job(self, source_id: str, table_filter: str) -> str:
        """Starts async "get columns" job and return job id."""
        url = urljoin(self.base_url, f'{source_id}/columns')
        params = {'table-filter': table_filter}
        response = self.session.post(url, params=params)
        response.raise_for_status()
        return response.json()['job_id']

    def get_job_status(self, job_id: str) -> str:
        """Get status of an async job."""
        url = urljoin(self.base_url, f'jobs/{job_id}')
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()['job_status']

    def get_job_results(self, job_id: str, offset: int = 0, limit: int = 500) -> List[Dict[str, Any]]:
        """Get results of an async job."""
        url = urljoin(self.base_url, f'jobs/{job_id}/results')
        params = {'offset': offset, 'limit': limit}
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()
