from typing import Any, Dict, List


class PanoModelField:
    """
    Pano Model Field
    """

    field_map: List[str]
    transformation: str
    data_type: str

    def __init__(self, field_map: List[str], transformation: str, data_type: str):
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


class PanoModelJoin:
    """
    Pano Model Field
    """

    field: str
    join_type: str
    relationship: str

    def __init__(self, field: str, join_type: str, relationship: str):
        self.field = field
        self.join_type = join_type
        self.relationship = relationship

    def to_dict(self) -> Dict[str, Any]:
        return {'field': self.field, 'join_type': self.join_type, 'relationship': self.relationship}

    @classmethod
    def from_dict(cls, inputs: Dict[str, Any]) -> 'PanoModelJoin':
        return cls(field=inputs['field'], join_type=inputs['join_type'], relationship=inputs['relationship'])


class PanoModel:
    """
    Pano Model
    """

    table_file_name: str
    data_source: str
    fields: List[PanoModelField]
    joins: List[PanoModelJoin]
    identifiers: List[str]
    api_version: str

    def __init__(
        self,
        table_file_name: str,
        data_source: str,
        fields: List[PanoModelField],
        joins: List[PanoModelJoin],
        identifiers: List[str],
        api_version: str,
    ):
        self.table_file_name = table_file_name
        self.data_source = data_source
        self.fields = fields
        self.joins = joins
        self.identifiers = identifiers
        self.api_version = api_version

    def to_dict(self) -> Dict[str, Any]:
        # The "table_file_name" is used as file name and not being exported
        return {
            'data_source': self.data_source,
            'fields': [x.to_dict() for x in self.fields],
            'joins': [x.to_dict() for x in self.joins],
            'identifiers': self.identifiers,
            'api_version': self.api_version,
        }

    @classmethod
    def from_dict(cls, inputs: Dict[str, Any]) -> 'PanoModel':
        return cls(
            table_file_name=inputs['table_file_name'],
            data_source=inputs['data_source'],
            fields=[PanoModelField.from_dict(x) for x in inputs.get('fields', [])],
            joins=[PanoModelJoin.from_dict(x) for x in inputs.get('joins', [])],
            identifiers=inputs.get('identifiers', []),
            api_version=inputs['api_version'],
        )


class PanoDataSource:
    """
    Pano Data Source
    """

    data_source_slug: str
    display_name: str

    def __init__(
        self, data_source_slug: str, display_name: str,
    ):
        self.data_source_slug = data_source_slug
        self.display_name = display_name

    def to_dict(self) -> Dict[str, Any]:
        return {
            'data_source_slug': self.data_source_slug,
            'display_name': self.display_name,
        }

    @classmethod
    def from_dict(cls, inputs: Dict[str, Any]) -> 'PanoDataSource':
        return cls(data_source_slug=inputs['data_source_slug'], display_name=inputs['display_name'])
