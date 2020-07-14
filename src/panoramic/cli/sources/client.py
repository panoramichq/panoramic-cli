from typing import Any, Dict, List, Optional

from panoramic.auth import OAuth2Client
from panoramic.cli.config.auth import get_client_id, get_client_secret
from panoramic.cli.config.source import get_base_url


class PhysicalDataSourceClient(OAuth2Client):

    """Metadata HTTP API client."""

    base_url: str
    _base_url_with_trailing_slash: str
    _company_id_query_params: Dict[str, str]

    def __init__(
        self, base_url: Optional[str] = None, client_id: Optional[str] = None, client_secret: Optional[str] = None
    ):
        client_id = client_id if client_id is not None else get_client_id()
        client_secret = client_secret if client_secret is not None else get_client_secret()

        # Since we need to request api/source?company_slug=x and api/virtual/slug?company_slug=x
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

    def get_sources(self, company_slug: str) -> List[Dict[str, Any]]:
        response = self.session.get(self.base_url, params={'company_slug': company_slug})
        response.raise_for_status()

        return response.json()['data']
