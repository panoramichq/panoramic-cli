from enum import Enum
from typing import List, Dict, Any


class DataSourceType(Enum):
    """
    Enumeration with all available datasource types
    """

    sql = 'sql'


class PanoModelDataSource:
    """
    Pano Model Data Source
    """

    path: str
    data_source_type: DataSourceType

    def __init__(self, path: str, data_source_type: DataSourceType):
        self.path = path
        self.data_source_type = data_source_type

    def to_dict(self) -> Dict[str, Any]:
        return {'path': self.path, 'data_source_type': self.data_source_type}


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


class PanoModel:
    """
    Pano Model
    """

    table_file_name: str
    data_source: PanoModelDataSource
    fields: List[PanoModelField]
    joins: List[PanoModelJoin]
    identifiers: List[str]
    api_version: str

    def __init__(
        self,
        table_file_name: str,
        data_source: PanoModelDataSource,
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
            'data_source': self.data_source.to_dict(),
            'fields': [x.to_dict() for x in self.fields],
            'joins': [x.to_dict() for x in self.joins],
            'identifiers': self.identifiers,
            'api_version': self.api_version,
        }
