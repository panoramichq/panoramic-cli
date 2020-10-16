import logging
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from panoramic.auth import OAuth2Client
from panoramic.cli.clients import VersionedClient
from panoramic.cli.config.auth import get_client_id, get_client_secret
from panoramic.cli.config.field import get_base_url

logger = logging.getLogger(__name__)


class Field:
    slug: str
    taxon_type: str
    display_name: str
    aggregation_type: Optional[str]

    def __init__(self, *, slug: str, taxon_type: str, display_name: str, aggregation_type: Optional[str] = None):
        self.slug = slug
        self.taxon_type = taxon_type
        self.display_name = display_name
        self.aggregation_type = aggregation_type

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Field':
        return cls(
            slug=data['slug'],
            taxon_type=data['taxon_type'],
            display_name=data['display_name'],
            aggregation_type=data.get('aggregation_type'),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            'slug': self.slug,
            'taxon_type': self.taxon_type,
            'display_name': self.display_name,
            'aggregation_type': self.aggregation_type,
        }

    def __hash__(self) -> int:
        return hash(self.to_dict())

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, Field):
            return False

        return self.to_dict() == o.to_dict()


class FieldClient(OAuth2Client, VersionedClient):

    """Model field HTTP API client."""

    base_url: str

    def __init__(
        self, base_url: Optional[str] = None, client_id: Optional[str] = None, client_secret: Optional[str] = None,
    ):
        base_url = base_url if base_url is not None else get_base_url()
        client_id = client_id if client_id is not None else get_client_id()
        client_secret = client_secret if client_secret is not None else get_client_secret()

        self.base_url = base_url
        super().__init__(client_id, client_secret)

    def create_field(self, company_slug: str, field: Field, data_source: Optional[str] = None):
        params = {'company_slug': company_slug, 'virtual_data_source': data_source}
        response = self.session.post(self.base_url, params=params, json=field.to_dict(), timeout=30)
        response.raise_for_status()

    def get_fields(
        self, company_slug: str, data_source: Optional[str] = None, offset: int = 0, limit: int = 100
    ) -> List[Field]:
        """Retrieve all fields in a given source and optionally a datasource."""
        logger.debug(f'Listing fields for company: {company_slug} source: {data_source}')
        params = {'company_slug': company_slug, 'virtual_data_source': data_source, 'offset': offset, 'limit': limit}
        response = self.session.get(self.base_url, params=params)
        response.raise_for_status()
        response_json = response.json()['data']
        return [Field.from_dict(d) for d in response_json]

    def update_fields(self, company_slug: str, fields: List[Field]):
        """Update given field."""
        logger.debug(f'Updating fields with slugs: {", ".join(f.slug for f in fields)}')
        params = {'company_slug': company_slug}
        response = self.session.put(self.base_url, json=[f.to_dict() for f in fields], params=params, timeout=30)
        response.raise_for_status()

    def delete_field(self, company_slug: str, field_slug: str, data_source: Optional[str] = None):
        """Delete a field with a given name."""
        url = urljoin(self.base_url, field_slug)
        logger.debug(f'Deleting model with name: {field_slug}')
        params = {'virtual_data_source': data_source, 'company_slug': company_slug}
        response = self.session.delete(url, params=params, timeout=30)
        response.raise_for_status()
