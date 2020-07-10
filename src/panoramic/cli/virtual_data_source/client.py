import logging
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests

from panoramic.auth import OAuth2Client
from panoramic.cli.config.auth import get_client_id, get_client_secret, get_token
from panoramic.cli.config.virtual_data_source import get_base_url

logger = logging.getLogger(__name__)


class VirtualDataSourceClient(OAuth2Client):

    """Metadata HTTP API client."""

    base_url: str
    _base_url_with_trailing_slash: str
    _company_id_query_params: Dict[str, str]

    def __init__(
        self,
        base_url: Optional[str] = None,
        token: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
    ):
        token = token if token is not None else get_token()
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

        if token:
            self.session = requests.Session()
            self.session.headers.update(**{'x-auth-token': token})
        else:
            super().__init__(client_id, client_secret)

    def upsert_virtual_data_source(self, company_id: str, company_slug: str, payload: Dict):
        """Create a virtual data source for a company"""
        logger.debug(f'Upserting virtual data source with payload {payload} under company {company_id}|{company_slug}')
        response = self.session.put(self.base_url, params={'company_id': company_id, 'company_slug': company_slug})
        response.raise_for_status()

    def get_virtual_data_source(self, company_id: str, company_slug: str, slug: str) -> Dict[str, Any]:
        """Retrieve a virtual data source"""
        logger.debug(f'Retrieving a virtual data source with slug {slug} under company {company_id}|{company_slug}')
        url = urljoin(self._base_url_with_trailing_slash, slug)
        response = self.session.get(url, params={'company_id': company_id, 'company_slug': company_slug})
        response.raise_for_status()
        return response.json()['data']

    def get_all_virtual_data_sources(self, company_id: str, company_slug: str) -> List[Dict[str, Any]]:
        """Retrieve all virtual data sources under a company"""
        logger.debug(f'Retrieving all virtual data sources under company {company_id}|{company_slug}')
        response = self.session.get(self.base_url, params={'company_id': company_id, 'company_slug': company_slug})
        response.raise_for_status()
        return response.json()['data']

    def delete_virtual_data_source(self, company_id: str, company_slug: str, slug: str):
        """Delete a virtual data source"""
        logger.debug(f'Deleting virtual data source with slug {slug} under company {company_id}|{company_slug}')
        url = urljoin(self._base_url_with_trailing_slash, slug)
        response = self.session.delete(url, params={'company_id': company_id, 'company_slug': company_slug})
        response.raise_for_status()
