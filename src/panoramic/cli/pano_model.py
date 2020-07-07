from typing import List


class PanoModelDataSource:
    """
    Pano Model Data Source
    """

    sql: str

    def __init__(self, sql: str):
        self.sql = sql

    def to_dict(self):
        return {'sql': self.sql}


class PanoModelField:
    """
    Pano Model Field
    """

    data_type: str
    transformation: str
    field_map: List[str]

    def __init__(self, data_type: str, transformation: str, field_map: List[str]):
        self.data_type = data_type
        self.transformation = transformation
        self.field_map = field_map

    def to_dict(self):
        return {'data_type': self.data_type, 'transformation': self.transformation, 'field_map': self.field_map}


class PanoModelJoin:
    """
    Pano Model Field
    """

    join: str

    def __init__(self, join: str):
        self.join = join

    def to_dict(self):
        return {'join': self.join}


class PanoModel:
    """
    Pano Model
    """

    table_file_id: str
    data_source: PanoModelDataSource
    fields: List[PanoModelField]
    joins: List[PanoModelJoin]
    identifiers: List[str]

    def __init__(
        self,
        table_file_id: str,
        data_source: PanoModelDataSource,
        fields: List[PanoModelField],
        joins: List[PanoModelJoin],
        identifiers: List[str],
    ):
        self.table_file_id = table_file_id
        self.data_source = data_source
        self.fields = fields
        self.joins = joins
        self.identifiers = identifiers

    def to_dict(self):
        return {
            'table_file_id': self.table_file_id,
            'data_source': self.data_source.to_dict(),
            'fields': [x.to_dict() for x in self.fields],
            'joins': [x.to_dict() for x in self.joins],
            'identifiers': self.identifiers,
        }
