import itertools
import logging
import operator

from typing import Dict, Iterable, List

import pydash

from panoramic.cli.pano_model import (
    DataSourceType,
    PanoModel,
    PanoModelDataSource,
    PanoModelField,
)


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
        return table_schema


def load_scanned_tables(raw_columns: Iterable[Dict]) -> List[PanoModel]:
    """
    Load result of metadata table columns scan into Table Model
    """
    models = []
    columns_grouped = itertools.groupby(raw_columns, operator.itemgetter('table_schema', 'table_name'))

    for (table_schema, table_name), columns in columns_grouped:
        fields = []
        schema_path = _remove_source_from_path(table_schema)
        table_path = '.'.join([schema_path, table_name])

        for col in columns:
            data_type = col['data_type']
            column_name = col['column_name']

            fields.append(
                PanoModelField(
                    data_type=data_type,
                    transformation=column_name,
                    field_map=[pydash.slugify('.'.join([table_path, column_name]), separator="_").lower()],
                )
            )

        models.append(
            PanoModel(
                table_file_name=pydash.slugify(table_path, separator="_").lower(),
                data_source=PanoModelDataSource(path=table_path, data_source_type=DataSourceType.sql.value),
                fields=fields,
                joins=[],
                identifiers=[],
            )
        )

    return models
