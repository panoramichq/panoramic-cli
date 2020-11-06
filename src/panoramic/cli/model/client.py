import logging
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from panoramic.auth import OAuth2Client
from panoramic.cli.clients import VersionedClient
from panoramic.cli.config.auth import get_client_id, get_client_secret
from panoramic.cli.config.model import get_base_url

logger = logging.getLogger(__name__)


class ModelField:

    field_map: List[str]
    data_reference: str

    def __init__(
        self,
        *,
        field_map: List[str],
        data_reference: str,
    ):
        self.field_map = field_map
        self.data_reference = data_reference

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ModelField':
        return cls(
            field_map=data['field_map'],
            data_reference=data['data_reference'],
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            'field_map': self.field_map,
            'data_reference': self.data_reference,
        }

    def __hash__(self) -> int:
        return hash(
            (
                tuple(self.field_map),
                self.data_reference,
            )
        )

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, ModelField):
            return False

        return self.to_dict() == o.to_dict()


class ModelJoin:

    to_model: str
    join_type: str
    relationship: str
    fields: List[str]

    def __init__(self, *, to_model: str, join_type: str, relationship: str, fields: List[str]):
        self.to_model = to_model
        self.join_type = join_type
        self.relationship = relationship
        self.fields = fields

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ModelJoin':
        return cls(
            to_model=data['to_model'],
            join_type=data['join_type'],
            relationship=data['relationship'],
            fields=data['fields'],
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            'to_model': self.to_model,
            'join_type': self.join_type,
            'relationship': self.relationship,
            'fields': self.fields,
        }

    def __hash__(self) -> int:
        return hash(
            (
                self.to_model,
                self.join_type,
                self.relationship,
                tuple(self.fields),
            )
        )

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, ModelJoin):
            return False

        return self.to_dict() == o.to_dict()


class Model:

    model_name: str
    data_source: str
    fields: List[ModelField]
    joins: List[ModelJoin]
    visibility: str
    virtual_data_source: Optional[str]
    identifiers: List[str]

    def __init__(
        self,
        *,
        model_name: str,
        data_source: str,
        fields: List[ModelField],
        joins: List[ModelJoin],
        identifiers: List[str],
        visibility: str,
        virtual_data_source: Optional[str] = None,
    ):
        self.model_name = model_name
        self.data_source = data_source
        self.fields = fields
        self.joins = joins
        self.identifiers = identifiers
        self.visibility = visibility
        self.virtual_data_source = virtual_data_source

    @classmethod
    def from_dict(cls, data: Dict[str, Any], **kwargs) -> 'Model':
        return cls(
            model_name=data['model_name'],
            data_source=data['data_source'],
            fields=[ModelField.from_dict(a) for a in data['fields']],
            joins=[ModelJoin.from_dict(d) for d in data['joins']],
            identifiers=data['identifiers'],
            visibility=data['visibility'],
            **kwargs,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            'model_name': self.model_name,
            'data_source': self.data_source,
            'fields': [a.to_dict() for a in self.fields],
            'joins': [j.to_dict() for j in self.joins],
            'identifiers': self.identifiers,
            'visibility': self.visibility,
        }

    def __hash__(self) -> int:
        return hash(
            (
                self.model_name,
                self.data_source,
                tuple(self.fields),
                tuple(self.joins),
                self.visibility,
                self.virtual_data_source,
                tuple(self.identifiers),
            )
        )

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, Model):
            return False

        return self.to_dict() == o.to_dict()


class ModelClient(OAuth2Client, VersionedClient):

    """Model HTTP API client."""

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

    def delete_model(self, data_source: str, company_slug: str, name: str):
        """Delete model with a given name."""
        url = urljoin(self.base_url, name)
        logger.debug(f'Deleting model with name: {name}')
        params = {'virtual_data_source': data_source, 'company_slug': company_slug}
        response = self.session.delete(url, params=params, timeout=30)
        response.raise_for_status()

    def upsert_model(self, data_source: str, company_slug: str, model: Model):
        """Add or update given model."""
        logger.debug(f'Upserting model with name: {model.model_name}')
        params = {'virtual_data_source': data_source, 'company_slug': company_slug}
        response = self.session.put(self.base_url, json=model.to_dict(), params=params, timeout=30)
        response.raise_for_status()

    def get_model_names(self, data_source: str, company_slug: str, offset: int = 0, limit: int = 100) -> List[str]:
        """Retrieve names of all models in a given source."""
        logger.debug(f'Listing names of models for source: {data_source}')
        url = urljoin(self.base_url, 'model-name')
        params = {'virtual_data_source': data_source, 'company_slug': company_slug, 'offset': offset, 'limit': limit}
        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()['data']

    def get_models(self, data_source: str, company_slug: str, offset: int = 0, limit: int = 100) -> List[Model]:
        """Retrieve all models in a given source."""
        logger.debug(f'Listing models for source: {data_source}')
        params = {'virtual_data_source': data_source, 'company_slug': company_slug, 'offset': offset, 'limit': limit}
        response = self.session.get(
            self.base_url,
            params=params,
        )
        response.raise_for_status()
        return [Model.from_dict(d, virtual_data_source=data_source) for d in response.json()['data']]
