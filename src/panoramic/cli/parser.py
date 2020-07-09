import itertools
import logging
import operator

from typing import Dict, Iterable, List, Set

import pydash

from panoramic.cli.pano_model import (
    DataSourceType,
    PanoModel,
    PanoModelDataSource,
    PanoModelField,
)
from panoramic.cli.util import generate_unique_slug


logger = logging.getLogger(__name__)

DREMIO_DELIMITER = '.'


def _remove_source_from_path(table_schema: str):
    """
    Return dremio path without the source id at the start. It can be an empty string in some cases.
    """
    if DREMIO_DELIMITER in table_schema:
        _, schema_path = table_schema.split(DREMIO_DELIMITER, 1)
        return schema_path
    else:
        return ''


def load_scanned_tables(raw_columns: Iterable[Dict], api_version: str) -> List[PanoModel]:
    """
    Load result of metadata table columns scan into Table Model
    """
    models = []
    columns_grouped = itertools.groupby(raw_columns, operator.itemgetter('table_schema', 'table_name'))

    for (table_schema, table_name), columns in columns_grouped:
        fields = []
        field_map_slugs: Set[str] = set()
        schema_path = _remove_source_from_path(table_schema)
        table_path = '.'.join([schema_path, table_name])

        for col in columns:
            data_type = col['data_type']
            column_name = col['column_name']

            field_map_item = generate_unique_slug('.'.join([table_path, column_name]), field_map_slugs)
            field_map_slugs.add(field_map_item)

            fields.append(PanoModelField(data_type=data_type, transformation=column_name, field_map=[field_map_item],))

        models.append(
            PanoModel(
                table_file_name=pydash.slugify(table_path, separator="_").lower(),
                data_source=PanoModelDataSource(path=table_path, data_source_type=DataSourceType.sql.value),
                fields=fields,
                joins=[],
                identifiers=[],
                api_version=api_version,
            )
        )

    return models
