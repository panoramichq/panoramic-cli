import logging
from typing import Optional
from urllib.parse import urljoin

from panoramic.auth import OAuth2Client
from panoramic.cli.clients import VersionedClient
from panoramic.cli.config.auth import get_client_id, get_client_secret
from panoramic.cli.config.transform import get_base_url
from panoramic.cli.transform.pano_transform import PanoTransform

logger = logging.getLogger(__name__)


class TransformClient(OAuth2Client, VersionedClient):

    """Transform HTTP API client."""

    base_url: str

    def __init__(
        self,
        base_url: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
    ):
        base_url = base_url if base_url is not None else get_base_url()
        client_id = client_id if client_id is not None else get_client_id()
        client_secret = client_secret if client_secret is not None else get_client_secret()

        self.base_url = base_url
        super().__init__(client_id, client_secret)

    def compile_transform(self, transform: PanoTransform, company_slug: str, connection_name: str) -> str:
        logger.debug(f'Compiling transform with name {transform.name}')
        url = urljoin(self.base_url, 'compile')
        response = self.session.post(
            url=url,
            json=transform.to_dict(),
            headers={'panoramic-husky-dremio-target': 'true'},
            params={'company_slug': company_slug, 'physical_data_source': connection_name},
        )
        response.raise_for_status()

        return response.json()['data']['sql']
