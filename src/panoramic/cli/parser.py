import logging
import re

from typing import Dict, Iterable, List

from panoramic.cli.errors import ParserException
from panoramic.cli.pano_model import (
    PanoModel,
    PanoModelDataSource,
    PanoModelField,
)
from panoramic.cli.util import peek_iterator


logger = logging.getLogger(__name__)


def _get_table_path(col: Dict) -> str:
    """
    Get full table path
    """
    return '.'.join([col['table_schema'], col['table_name']])


def _get_column_path(col: Dict) -> str:
    """
    Get full table path
    """
    return '.'.join([col['table_schema'], col['table_name'], col['column_name']])


def _get_field_data_type(col: Dict) -> str:
    """
    Get column data type
    """
    # TODO: What is the correct format? Talk to @jakub
    # TODO: Escape
    return col['data_type']


def _get_field_transformation(col: Dict) -> str:
    """
    Get column data type
    """
    # TODO: What is the correct format? Talk to @jakub
    # TODO: Escape
    return col['table_name']


def _get_field_map(col: Dict) -> List[str]:
    """
    Get column data type
    """
    # TODO: What is the correct format? Talk to @jakub
    # TODO: Escape
    return [_get_column_path(col)]


def _get_table_file_id(col: Dict) -> str:
    """
    Attempt to get unique and fs/url safe file name from table path
    """
    # TODO: Make it unique, figure out details with @jakub
    # TODO: Make it safe for fs/url, figure out details with @jakub
    return re.sub(r'\W+', '', _get_table_path(col))


def load_scanned_table(raw_columns: Iterable[Dict]) -> PanoModel:
    """
    Load result of metadata table columns scan into Table Model
    """
    col, raw_columns = peek_iterator(raw_columns)
    table_path = _get_table_path(col)
    table_file_id = _get_table_file_id(col)
    data_source = PanoModelDataSource(sql=table_path)
    fields = []

    for col in raw_columns:
        if table_path != _get_table_path(col):
            raise ParserException('Unable to parse columns from multiple tables')

        fields.append(
            PanoModelField(
                data_type=_get_field_data_type(col),
                transformation=_get_field_transformation(col),
                field_map=_get_field_map(col),
            )
        )

    return PanoModel(table_file_id=table_file_id, data_source=data_source, fields=fields, joins=[], identifiers=[])
