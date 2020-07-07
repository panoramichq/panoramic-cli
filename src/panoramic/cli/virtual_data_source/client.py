import logging
from dataclasses import dataclass

from typing import Any, Dict, Optional, List

from urllib.parse import urljoin

from panoramic.auth import OAuth2Client
from panoramic.cli.config.auth import get_client_id, get_client_secret
from panoramic.cli.config.virtual_data_source import get_base_url


logger = logging.getLogger(__name__)


class VirtualDataSource:
    slug: str
    display_name: str

    def __init__(self, slug: str, display_name: str):
        self.slug = slug
        self.display_name = display_name

    @classmethod
    def from_dict(cls, data: Dict) -> 'VirtualDataSource':
        return cls(slug=data['slug'], display_name=data['display_name'])


class VirtualDataSourceClient(OAuth2Client):

    """Metadata HTTP API client."""

    base_url: str

    def __init__(
        self,
        company_id: str,
        base_url: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
    ):
        base_url = base_url if base_url is not None else get_base_url()
        client_id = client_id if client_id is not None else get_client_id()
        client_secret = client_secret if client_secret is not None else get_client_secret()

        super().__init__(client_id, client_secret)
        self.base_url = base_url
        self.company_id = company_id
        self._company_id_query_params = {'company_id': self.company_id}

    def create(self, payload: Dict) -> Any:
        pass

    def get(self, slug: str) -> VirtualDataSource:
        url = urljoin(self.base_url, f'/{slug}')
        response = self.session.get(url, params=self._company_id_query_params)
        response.raise_for_status()
        return response.json()['data']

    def all(self) -> List[Dict]:
        response = self.session.get(self.base_url, params=self._company_id_query_params)
        response.raise_for_status()
        return response.json()['data']

    def update(self, slug: str, payload: Any):
        url = urljoin(self.base_url, slug)
        pass

    def delete(self, slug: str):
        url = urljoin(self.base_url, f'/{slug}')
        response = self.session.delete(url, params=self._company_id_query_params)
        response.raise_for_status()


if __name__ == "__main__":
    vdsc = VirtualDataSourceClient(company_id='50')
    test = vdsc.all()
    print('test')
