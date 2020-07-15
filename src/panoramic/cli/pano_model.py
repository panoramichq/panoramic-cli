from abc import ABC
from typing import Any, Dict, List, Optional


class Actionable(ABC):

    """Interface for object that you can perform actions on."""

    package: Optional[str]


class PanoModelField:
    """Field stored on a model."""

    field_map: List[str]
    transformation: str
    data_type: str

    def __init__(self, *, field_map: List[str], transformation: str, data_type: str):
        self.field_map = field_map
        self.transformation = transformation
        self.data_type = data_type

    def to_dict(self) -> Dict[str, Any]:
        return {'field_map': self.field_map, 'transformation': self.transformation, 'data_type': self.data_type}

    @classmethod
    def from_dict(cls, inputs: Dict[str, Any]) -> 'PanoModelField':
        return cls(
            field_map=inputs.get('field_map', []),
            transformation=inputs['transformation'],
            data_type=inputs['data_type'],
        )

    def __hash__(self) -> int:
        return hash(self.to_dict())

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, self.__class__):
            return False

        return self.to_dict() == o.to_dict()


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
            'field': self.fields,
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

    def __hash__(self) -> int:
        return hash(self.to_dict())

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, self.__class__):
            return False

        return self.to_dict() == o.to_dict()


class PanoModel(Actionable):
    """Model representing some table."""

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
    ):
        self.model_name = model_name
        self.data_source = data_source
        self.fields = fields
        self.joins = joins
        self.identifiers = identifiers
        self.virtual_data_source = virtual_data_source
        self.package = package

    @property
    def id(self) -> Any:
        return (self.virtual_data_source, self.model_name)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'model_name': self.model_name,
            'data_source': self.data_source,
            'fields': [x.to_dict() for x in self.fields],
            'joins': [x.to_dict() for x in self.joins],
            'identifiers': self.identifiers,
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
        )

    def __hash__(self) -> int:
        return hash(self.to_dict())

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, self.__class__):
            return False

        return self.to_dict() == o.to_dict()


class PanoVirtualDataSource(Actionable):
    """Group collection of models into one data source."""

    dataset_slug: str
    display_name: str

    def __init__(self, *, dataset_slug: str, display_name: str, package: Optional[str] = None):
        self.dataset_slug = dataset_slug
        self.display_name = display_name
        self.package = package

    @property
    def id(self) -> str:
        return self.dataset_slug

    def to_dict(self) -> Dict[str, Any]:
        return {
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
        return hash(self.to_dict())

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, self.__class__):
            return False

        return self.to_dict() == o.to_dict()
