import logging
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests

from panoramic.auth import OAuth2Client
from panoramic.cli.config.auth import get_client_id, get_client_secret, get_token
from panoramic.cli.config.model import get_base_url

logger = logging.getLogger(__name__)


class ModelAttribute:

    column_data_type: Optional[str]
    taxon: str
    identifier: bool
    transformation: str

    def __init__(
        self, *, column_data_type: Optional[str], taxon: str, identifier: bool, transformation: str,
    ):
        self.column_data_type = column_data_type
        self.taxon = taxon
        self.identifier = identifier
        self.transformation = transformation

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ModelAttribute':
        return cls(
            column_data_type=data.get('column_data_type'),
            taxon=data['taxon'],
            identifier=data['identifier'],
            transformation=data['transformation'],
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            'column_data_type': self.column_data_type,
            'taxon': self.taxon,
            'identifier': self.identifier,
            'transformation': self.transformation,
        }

    def __hash__(self) -> int:
        return hash(self.to_dict())

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, self.__class__):
            return False

        return self.to_dict() == o.to_dict()


class ModelJoin:

    to_model: str
    join_type: str
    relationship: str
    taxons: List[str]

    def __init__(self, *, to_model: str, join_type: str, relationship: str, taxons: List[str]):
        self.to_model = to_model
        self.join_type = join_type
        self.relationship = relationship
        self.taxons = taxons

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ModelJoin':
        return cls(
            to_model=data['to_model'],
            join_type=data['join_type'],
            relationship=data['relationship'],
            taxons=data['taxons'],
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            'to_model': self.to_model,
            'join_type': self.join_type,
            'relationship': self.relationship,
            'taxons': self.taxons,
        }

    def __hash__(self) -> int:
        return hash(self.to_dict())

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, self.__class__):
            return False

        return self.to_dict() == o.to_dict()


class Model:

    name: str
    fully_qualified_object_name: str
    attributes: List[ModelAttribute]
    joins: List[ModelJoin]
    visibility: str
    virtual_data_source: Optional[str]

    def __init__(
        self,
        *,
        name: str,
        fully_qualified_object_name: str,
        attributes: List[ModelAttribute],
        joins: List[ModelJoin],
        visibility: str,
        virtual_data_source: Optional[str] = None,
    ):
        self.name = name
        self.fully_qualified_object_name = fully_qualified_object_name
        self.attributes = attributes
        self.joins = joins
        self.visibility = visibility
        self.virtual_data_source = virtual_data_source

    @classmethod
    def from_dict(cls, data: Dict[str, Any], **kwargs) -> 'Model':
        return cls(
            name=data['name'],
            fully_qualified_object_name=data['fully_qualified_object_name'],
            attributes=[ModelAttribute.from_dict(a) for a in data['attributes']],
            joins=[ModelJoin.from_dict(d) for d in data['joins']],
            visibility=data['visibility'],
            **kwargs,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'fully_qualified_object_name': self.fully_qualified_object_name,
            'attributes': [a.to_dict() for a in self.attributes],
            'joins': [j.to_dict() for j in self.joins],
            'visibility': self.visibility,
        }

    def __hash__(self) -> int:
        return hash(self.to_dict())

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, self.__class__):
            return False

        return self.to_dict() == o.to_dict()


class ModelClient(OAuth2Client):

    """Model HTTP API client."""

    base_url: str

    def __init__(
        self, base_url: Optional[str] = None, client_id: Optional[str] = None, client_secret: Optional[str] = None,
    ):
        token = get_token()
        base_url = base_url if base_url is not None else get_base_url()
        client_id = client_id if client_id is not None else get_client_id()
        client_secret = client_secret if client_secret is not None else get_client_secret()

        self.base_url = base_url

        if token is not None:
            self.session = requests.Session()
            self.session.headers.update(**{'x-auth-token': token})
        else:
            super().__init__(client_id, client_secret)

    def delete_model(self, data_source: str, company_slug: str, name: str):
        """Delete model with a given name."""
        url = urljoin(self.base_url, name)
        logger.debug(f'Deleting model with name: {name}')
        # TODO: change once company_slug works on API layer
        # params = {'virtual_data_source': data_source, 'company_slug': company_slug}
        params = {'virtual_data_source': 'my_facebook', 'company_slug': 'operam', 'company_id': '1'}
        response = self.session.delete(url, params=params, timeout=5)
        response.raise_for_status()

    def upsert_model(self, data_source: str, company_slug: str, model: Model):
        """Add or update given model."""
        logger.debug(f'Upserting model with name: {model.name}')
        # TODO: change once company_slug works on API layer
        # params = {'virtual_data_source': data_source, 'company_slug': company_slug}
        params = {'virtual_data_source': 'my_facebook', 'company_slug': 'operam', 'company_id': '1'}
        response = self.session.put(self.base_url, json=model.to_dict(), params=params, timeout=5)
        response.raise_for_status()

    def get_model_names(self, data_source: str, company_slug: str, offset: int = 0, limit: int = 100) -> List[str]:
        """Retrieve names of all models in a given source."""
        logger.debug(f'Listing names of models for source: {data_source}')
        url = urljoin(self.base_url, 'model-name')
        # TODO: change once company_slug works on API layer
        # params = {'virtual_data_source': data_source, 'company_slug': company_slug, 'offset': offset, 'limit': limit}
        params = {
            'virtual_data_source': 'my_facebook',
            'company_id': '1',
            'company_slug': 'operam',
            'offset': offset,
            'limit': limit,
        }
        response = self.session.get(url, params=params, timeout=5)
        response.raise_for_status()
        return response.json()['data']

    def get_models(self, data_source: str, company_slug: str, offset: int = 0, limit: int = 100) -> List[Model]:
        """Retrieve all models in a given source."""
        logger.debug(f'Listing models for source: {data_source}')
        # params = {'virtual_data_source': data_source, 'company_slug': company_slug, 'offset': offset, 'limit': limit}
        params = {
            'virtual_data_source': 'my_facebook',
            'company_id': '1',
            'company_slug': 'operam',
            'offset': offset,
            'limit': limit,
        }
        response = self.session.get(self.base_url, params=params,)
        response.raise_for_status()
        return [Model.from_dict(d, virtual_data_source=data_source) for d in response.json()['data']]
