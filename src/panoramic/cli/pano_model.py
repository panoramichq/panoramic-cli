from abc import ABC
from typing import Any, Dict, List, Optional, Tuple, Union

from panoramic.cli.field.client import Aggregation


class Actionable(ABC):

    """Interface for object that you can perform actions on."""

    package: Optional[str] = None

    file_name: Optional[str] = None

    @property
    def id(_) -> Any:
        raise NotImplementedError('id not implemented for base class')

    def to_dict(self) -> Dict[str, Any]:
        raise NotImplementedError('to_dict not implemented for base class')

    def __hash__(self) -> int:
        raise NotImplementedError('__hash__ not implemented for base class')

    def __eq__(self, o: object) -> bool:
        raise NotImplementedError('__eq__ not implemented for base class')


class PanoModelField:
    """Field stored on a model."""

    field_map: List[str]
    data_reference: str

    def __init__(self, *, field_map: List[str], data_reference: str):
        self.field_map = field_map
        self.data_reference = data_reference

    def to_dict(self) -> Dict[str, Any]:
        return {
            'field_map': self.field_map,
            'data_reference': self.data_reference,
        }

    @classmethod
    def from_dict(cls, inputs: Dict[str, Any]) -> 'PanoModelField':
        return cls(
            field_map=inputs['field_map'],
            data_reference=inputs['data_reference'],
        )

    def identifier(self) -> str:
        return f'{self.data_reference}_{",".join(sorted(self.field_map))}'

    def __hash__(self) -> int:
        return hash(
            (
                tuple(self.field_map),
                self.data_reference,
            )
        )

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, PanoModelField):
            return False

        return self.to_dict() == o.to_dict()


class PanoField(Actionable):
    """Field definition."""

    API_VERSION = 'v1'

    slug: str
    group: str
    display_name: str
    data_type: str
    field_type: str
    description: Optional[str]
    data_source: Optional[str]
    calculation: Optional[str]
    display_format: Optional[str]
    aggregation: Optional[Aggregation]

    def __init__(
        self,
        *,
        slug: str,
        group: str,
        display_name: str,
        data_type: str,
        field_type: str,
        description: Optional[str] = None,
        data_source: Optional[str] = None,
        calculation: Optional[str] = None,
        display_format: Optional[str] = None,
        # TODO: create Pano class for aggregation
        aggregation: Optional[Aggregation] = None,
        package: Optional[str] = None,
        file_name: Optional[str] = None,
    ):
        self.slug = slug
        self.group = group
        self.display_name = display_name
        self.data_type = data_type
        self.field_type = field_type
        self.description = description
        self.data_source = data_source
        self.calculation = calculation
        self.display_format = display_format
        self.aggregation = aggregation
        self.package = package
        self.file_name = file_name

    def to_dict(self) -> Dict[str, Any]:
        # data_source is not persisted to YAML
        return {k: v for k, v in self._to_internal_dict().items() if k != 'data_source'}

    def _to_internal_dict(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            'api_version': self.API_VERSION,
            'slug': self.slug,
            'group': self.group,
            'display_name': self.display_name,
            'data_type': self.data_type,
            'field_type': self.field_type,
            'data_source': self.data_source,
        }

        if self.description is not None:
            data['description'] = self.description
        if self.calculation is not None:
            data['calculation'] = self.calculation
        if self.aggregation is not None:
            data['aggregation'] = self.aggregation.to_dict()
        if self.display_format is not None:
            data['display_format'] = self.display_format

        return data

    @classmethod
    def from_dict(cls, inputs: Dict[str, Any]) -> 'PanoField':
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
            package=inputs.get('package'),
            file_name=inputs.get('file_name'),
        )

    @property
    def id(self) -> Union[Tuple[str], Tuple[str, str]]:
        if self.data_source is not None:
            return (self.data_source, self.slug)
        else:
            return (self.slug,)

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
                self.display_format,
                self.aggregation,
                self.package,
                self.file_name,
            )
        )

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, PanoField):
            return False

        return self._to_internal_dict() == o._to_internal_dict()


class PanoModelJoin:
    """Represent joins on other models."""

    fields: List[str]
    join_type: str
    relationship: str
    to_model: str

    def __init__(self, *, fields: List[str], join_type: str, relationship: str, to_model: str):
        self.fields = fields
        self.join_type = join_type
        self.relationship = relationship
        self.to_model = to_model

    def to_dict(self) -> Dict[str, Any]:
        return {
            'fields': self.fields,
            'join_type': self.join_type,
            'relationship': self.relationship,
            'to_model': self.to_model,
        }

    @classmethod
    def from_dict(cls, inputs: Dict[str, Any]) -> 'PanoModelJoin':
        return cls(
            fields=inputs['fields'],
            join_type=inputs['join_type'],
            relationship=inputs['relationship'],
            to_model=inputs['to_model'],
        )

    def identifier(self) -> str:
        return f'{self.join_type}_{self.relationship}_{self.to_model}_{",".join(sorted(self.fields))}'

    def __hash__(self) -> int:
        return hash(
            (
                tuple(self.fields),
                self.join_type,
                self.relationship,
                self.to_model,
            )
        )

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, PanoModelJoin):
            return False

        return self.to_dict() == o.to_dict()


class PanoModel(Actionable):
    """Model representing some table."""

    API_VERSION = 'v1'

    model_name: str
    data_source: str
    fields: List[PanoModelField]
    joins: List[PanoModelJoin]
    identifiers: List[str]
    virtual_data_source: Optional[str]

    def __init__(
        self,
        *,
        model_name: str,
        data_source: str,
        fields: List[PanoModelField],
        joins: List[PanoModelJoin],
        identifiers: List[str],
        virtual_data_source: Optional[str] = None,
        package: Optional[str] = None,
        file_name: Optional[str] = None,
    ):
        self.model_name = model_name
        self.data_source = data_source
        self.fields = fields
        self.joins = joins
        self.identifiers = identifiers
        self.virtual_data_source = virtual_data_source
        self.package = package
        self.file_name = file_name

    @property
    def id(self) -> Tuple[Optional[str], str]:
        return (self.virtual_data_source, self.model_name)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'api_version': self.API_VERSION,
            'model_name': self.model_name,
            'data_source': self.data_source,
            'fields': [x.to_dict() for x in sorted(self.fields, key=lambda field: field.identifier())],
            'joins': [x.to_dict() for x in sorted(self.joins, key=lambda join: join.identifier())],
            'identifiers': sorted(self.identifiers),
            # The virtual_data_source and package are not exported to yaml
        }

    @classmethod
    def from_dict(cls, inputs: Dict[str, Any]) -> 'PanoModel':
        return cls(
            model_name=inputs['model_name'],
            data_source=inputs['data_source'],
            fields=[PanoModelField.from_dict(x) for x in inputs.get('fields', [])],
            joins=[PanoModelJoin.from_dict(x) for x in inputs.get('joins', [])],
            identifiers=inputs.get('identifiers', []),
            virtual_data_source=inputs.get('virtual_data_source'),
            package=inputs.get('package'),
            file_name=inputs.get('file_name'),
        )

    def __hash__(self) -> int:
        return hash(
            (
                self.model_name,
                self.data_source,
                tuple(self.fields),
                tuple(self.joins),
                tuple(self.identifiers),
                self.virtual_data_source,
            )
        )

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, PanoModel):
            return False

        return self.to_dict() == o.to_dict()


class PanoVirtualDataSource(Actionable):
    """Group collection of models into one data source."""

    API_VERSION = 'v1'

    dataset_slug: str
    display_name: str

    def __init__(self, *, dataset_slug: str, display_name: str, package: Optional[str] = None):
        self.dataset_slug = dataset_slug
        self.display_name = display_name
        self.package = package

    @property
    def id(self) -> Tuple[str]:
        return (self.dataset_slug,)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'api_version': self.API_VERSION,
            'dataset_slug': self.dataset_slug,
            'display_name': self.display_name,
            # The package is not exported to yaml
        }

    @classmethod
    def from_dict(cls, inputs: Dict[str, Any]) -> 'PanoVirtualDataSource':
        return cls(
            dataset_slug=inputs['dataset_slug'], display_name=inputs['display_name'], package=inputs.get('package')
        )

    def __hash__(self) -> int:
        return hash(
            (
                self.dataset_slug,
                self.display_name,
            )
        )

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, PanoVirtualDataSource):
            return False

        return self.to_dict() == o.to_dict()
