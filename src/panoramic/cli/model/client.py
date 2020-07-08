import logging
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from panoramic.auth import OAuth2Client
from panoramic.cli.config.auth import get_client_id, get_client_secret
from panoramic.cli.config.model import get_base_url

logger = logging.getLogger(__name__)


class ModelClient(OAuth2Client):

    """Model HTTP API client."""

    base_url: str

    def __init__(
        self, base_url: Optional[str] = None, client_id: Optional[str] = None, client_secret: Optional[str] = None,
    ):
        base_url = base_url if base_url is not None else get_base_url()
        client_id = client_id if client_id is not None else get_client_id()
        client_secret = client_secret if client_secret is not None else get_client_secret()

        super().__init__(client_id, client_secret)
        self.base_url = base_url

    def delete_model(self, data_source: str, company_id: str, name: str):
        """Delete model with a given name."""
        url = urljoin(self.base_url, name)
        logger.debug(f'Deleting model with name: {name}')
        params = {'virtual_data_source': data_source, 'company_id': company_id}
        response = self.session.delete(url, params=params, timeout=5)
        response.raise_for_status()

    def upsert_model(self, data_source: str, company_id: str, model: Dict[str, Any]):
        """Add or update given model."""
        logger.debug(f'Upserting model with name: {model["name"]}')
        params = {'virtual_data_source': data_source, 'company_id': company_id}
        response = self.session.put(self.base_url, params=params, timeout=5)
        response.raise_for_status()

    def get_model_names(self, data_source: str, company_id: str, offset: int = 0, limit: int = 100) -> List[str]:
        """Retrieve names of all models in a given source."""
        logger.debug(f'Listing names of models for source: {data_source}')
        url = urljoin(self.base_url, 'model-name')
        params = {'virtual_data_source': data_source, 'company_id': company_id, 'offset': offset, 'limit': limit}
        response = self.session.get(url, params=params, timeout=5)
        response.raise_for_status()
        return response.json()['data']

    def get_models(self, data_source: str, company_id: str, offset: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
        """Retrieve all models in a given source."""
        logger.debug(f'Listing names of models for source: {data_source}')
        params = {'virtual_data_source': data_source, 'company_id': company_id, 'offset': offset, 'limit': limit}
        response = self.session.get(self.base_url, params=params,)
        response.raise_for_status()
        return response.json()['data']
