from typing import Any, Dict, List, Optional

from panoramic.auth.client import OAuth2Client
from panoramic.cli.clients import VersionedClient
from panoramic.cli.config.auth import get_client_id, get_client_secret
from panoramic.cli.config.source import get_base_url


class PhysicalDataSourceClient(OAuth2Client, VersionedClient):

    """Physical data source HTTP API client."""

    base_url: str

    def __init__(
        self, base_url: Optional[str] = None, client_id: Optional[str] = None, client_secret: Optional[str] = None
    ):
        client_id = client_id if client_id is not None else get_client_id()
        client_secret = client_secret if client_secret is not None else get_client_secret()

        self.base_url = base_url if base_url is not None else get_base_url()

        super().__init__(client_id, client_secret)

    def get_sources(self, company_slug: str) -> List[Dict[str, Any]]:
        response = self.session.get(self.base_url, params={'company_slug': company_slug}, timeout=30)
        response.raise_for_status()
        return response.json()['data']
