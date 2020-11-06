import json
import logging
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from panoramic.auth import OAuth2Client
from panoramic.cli.clients import VersionedClient
from panoramic.cli.config.auth import get_client_id, get_client_secret
from panoramic.cli.config.field import get_base_url

logger = logging.getLogger(__name__)


class Aggregation:

    type: str
    params: Optional[Dict[str, Any]]

    def __init__(
        self,
        *,
        type: str,
        params: Optional[Dict[str, Any]],
    ):
        self.type = type
        self.params = params

    @classmethod
    def from_dict(cls, inputs: Dict[str, Any]) -> 'Aggregation':
        return cls(type=inputs['type'], params=inputs.get('params'))

    def to_dict(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {'type': self.type}
        if self.params is not None:
            data['params'] = self.params
        return data

    def __hash__(self) -> int:
        return hash(json.dumps(self.to_dict()))

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, Aggregation):
            return False

        return self.to_dict() == o.to_dict()


class Field:

    slug: str
    group: str
    display_name: str
    data_type: str
    field_type: str
    description: Optional[str]
    data_source: Optional[str]
    calculation: Optional[str]
    aggregation: Optional[Aggregation]
    display_format: Optional[str]

    def __init__(
        self,
        *,
        slug: str,
        group: str,
        display_name: str,
        data_type: str,
        field_type: str,
        description: Optional[str],
        data_source: Optional[str],
        calculation: Optional[str],
        aggregation: Optional[Aggregation],
        display_format: Optional[str],
    ):
        self.slug = slug
        self.group = group
        self.display_name = display_name
        self.data_type = data_type
        self.field_type = field_type
        self.description = description
        self.data_source = data_source
        self.calculation = calculation
        self.aggregation = aggregation
        self.display_format = display_format

    @classmethod
    def from_dict(cls, inputs: Dict[str, Any]) -> 'Field':
        aggregation = inputs.get('aggregation')
        return cls(
            slug=inputs['slug'],
            group=inputs['group'],
            display_name=inputs['display_name'],
            data_type=inputs['data_type'],
            field_type=inputs['field_type'],
            description=inputs.get('description'),
            data_source=inputs.get('data_source'),
            aggregation=Aggregation.from_dict(aggregation) if aggregation is not None else None,
            calculation=inputs.get('calculation'),
            display_format=inputs.get('display_format'),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            'slug': self.slug,
            'group': self.group,
            'display_name': self.display_name,
            'data_type': self.data_type,
            'field_type': self.field_type,
            'description': self.description,
            'calculation': self.calculation,
            'aggregation': self.aggregation.to_dict() if self.aggregation is not None else None,
            'data_source': self.data_source,
            'display_format': self.display_format,
        }

    def __hash__(self) -> int:
        return hash(
            (
                self.slug,
                self.group,
                self.display_name,
                self.data_type,
                self.field_type,
                self.description,
                self.data_source,
                self.calculation,
                self.aggregation,
                self.display_format,
            )
        )

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, Field):
            return False

        return self.to_dict() == o.to_dict()


class FieldClient(OAuth2Client, VersionedClient):

    """Model field HTTP API client."""

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

    def upsert_fields(self, company_slug: str, fields: List[Field], data_source: Optional[str] = None):
        params = {'company_slug': company_slug, 'virtual_data_source': data_source}
        response = self.session.post(
            self.base_url, params=params, json=[field.to_dict() for field in fields], timeout=30
        )
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

    def delete_fields(self, company_slug: str, slugs: List[str]):
        """Delete a field with a given name."""
        url = urljoin(self.base_url, 'delete')
        logger.debug(f'Deleting fields : {", ".join(slugs)}')
        response = self.session.post(url, json=slugs, params={'company_slug': company_slug}, timeout=30)
        response.raise_for_status()
