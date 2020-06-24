from typing import Any, Optional
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

    def get_columns(self, source_id: str, scope: str, page: int = 0, limit: int = 100) -> Any:
        url = urljoin(self.base_url, f'columns/{source_id}')
        params = {'scope': scope, 'page': page, 'limit': limit}
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()
