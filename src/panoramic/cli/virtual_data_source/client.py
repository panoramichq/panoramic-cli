import logging
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from panoramic.auth import OAuth2Client
from panoramic.cli.clients import VersionedClient
from panoramic.cli.config.auth import get_client_id, get_client_secret
from panoramic.cli.config.virtual_data_source import get_base_url

logger = logging.getLogger(__name__)


class VirtualDataSource:

    slug: str
    display_name: str

    def __init__(self, *, slug: str, display_name: str):
        self.slug = slug
        self.display_name = display_name

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'VirtualDataSource':
        return VirtualDataSource(slug=data['slug'], display_name=data['display_name'])

    def to_dict(self) -> Dict[str, Any]:
        return {
            'slug': self.slug,
            'display_name': self.display_name,
        }

    def __hash__(self) -> int:
        return hash(
            (
                self.slug,
                self.display_name,
            )
        )

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, VirtualDataSource):
            return False

        return self.to_dict() == o.to_dict()


class VirtualDataSourceClient(OAuth2Client, VersionedClient):

    """Metadata HTTP API client."""

    base_url: str
    _base_url_with_trailing_slash: str

    def __init__(
        self,
        base_url: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
    ):
        client_id = client_id if client_id is not None else get_client_id()
        client_secret = client_secret if client_secret is not None else get_client_secret()

        # Since we need to request api/virtual?company_id=x and api/virtual/slug?company_id=1
        # the base gets corrected to not include trailing slash
        #
        # Check https://stackoverflow.com/questions/10893374/python-confusions-with-urljoin for more context
        self.base_url = base_url if base_url is not None else get_base_url()
        if self.base_url[-1] == '/':
            self._base_url_with_trailing_slash = self.base_url
            self.base_url = self.base_url[0:-1]
        else:
            # base_url is in it's correct form - without trailing slash
            self._base_url_with_trailing_slash = self.base_url + '/'

        super().__init__(client_id, client_secret)

    def upsert_virtual_data_source(self, company_slug: str, payload: VirtualDataSource):
        """Create a virtual data source for a company"""
        logger.debug(f'Upserting virtual data source with payload {payload} under company {company_slug}')
        params = {'company_slug': company_slug}
        response = self.session.put(self.base_url, json=payload.to_dict(), params=params, timeout=30)
        response.raise_for_status()

    def get_virtual_data_source(self, company_slug: str, slug: str) -> VirtualDataSource:
        """Retrieve a virtual data source"""
        logger.debug(f'Retrieving a virtual data source with slug {slug} under company {company_slug}')
        url = urljoin(self._base_url_with_trailing_slash, slug)
        params = {'company_slug': company_slug}
        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return VirtualDataSource.from_dict(response.json()['data'])

    def get_all_virtual_data_sources(
        self, company_slug: str, *, offset: int = 0, limit: int = 100
    ) -> List[VirtualDataSource]:
        """Retrieve all virtual data sources under a company"""
        logger.debug(f'Retrieving all virtual data sources under company {company_slug}')
        params = {'company_slug': company_slug, 'offset': offset, 'limit': limit}
        response = self.session.get(self.base_url, params=params, timeout=30)
        response.raise_for_status()
        return [VirtualDataSource.from_dict(d) for d in response.json()['data']]

    def delete_virtual_data_source(self, company_slug: str, slug: str):
        """Delete a virtual data source"""
        logger.debug(f'Deleting virtual data source with slug {slug} under company {company_slug}')
        params = {'company_slug': company_slug}
        url = urljoin(self._base_url_with_trailing_slash, slug)
        response = self.session.delete(url, params=params, timeout=30)
        response.raise_for_status()
