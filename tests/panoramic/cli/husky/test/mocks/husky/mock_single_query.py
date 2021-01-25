from sqlalchemy import text
from sqlalchemy.sql import Select, column

from panoramic.cli.husky.service.constants import HUSKY_QUERY_DATA_SOURCE_COLUMN_NAME


def create_single_query_mock(data_source_name):
    """
    convenience fn to create sqlalchemy's Select clausewith some column and table.
    """
    return Select(
        columns=[column(data_source_name + '_column_mock'), column(HUSKY_QUERY_DATA_SOURCE_COLUMN_NAME)],
        from_obj=text(data_source_name + '_table_mock'),
    )
