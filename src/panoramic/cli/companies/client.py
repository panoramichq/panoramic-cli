from typing import Optional, Set

from panoramic.auth import OAuth2Client
from panoramic.cli.clients import VersionedClient
from panoramic.cli.config.auth import get_client_id, get_client_secret
from panoramic.cli.config.companies import get_base_url


class CompaniesClient(OAuth2Client, VersionedClient):

    """Companies HTTP API client."""

    base_url: str

    def __init__(
        self, base_url: Optional[str] = None, client_id: Optional[str] = None, client_secret: Optional[str] = None
    ):
        base_url = base_url if base_url is not None else get_base_url()
        client_id = client_id if client_id is not None else get_client_id()
        client_secret = client_secret if client_secret is not None else get_client_secret()

        super().__init__(client_id, client_secret)
        self.base_url = base_url

    def get_companies(self) -> Set[str]:
        response = self.session.get(self.base_url, timeout=30)
        response.raise_for_status()
        return set(response.json()['data'])
