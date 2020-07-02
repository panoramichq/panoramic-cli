from typing import List


class TableModelField:
    """
    Table Model Field
    """

    data_type: str
    sql_select: str
    field_map: List[str]

    def __init__(self, data_type: str, sql_select: str, field_map: List[str]):
        self.data_type = data_type
        self.sql_select = sql_select
        self.field_map = field_map


class TableModel:
    """
    Table Model
    """

    table_file_id: str
    source_path: str
    fields: List[TableModelField]
    joins: List[str]
    identifiers: List[str]

    def __init__(
        self,
        table_file_id: str,
        source_path: str,
        fields: List[TableModelField],
        joins: List[str],
        identifiers: List[str],
    ):
        self.table_file_id = table_file_id
        self.source_path = source_path
        self.fields = fields
        self.joins = joins
        self.identifiers = identifiers
