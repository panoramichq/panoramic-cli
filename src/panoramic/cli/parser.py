import itertools
import logging
import operator

from typing import Dict, Iterable, List

import pydash

from panoramic.cli.errors import MissingSchemaException
from panoramic.cli.pano_model import PanoModel
from panoramic.cli.util import slug_string


logger = logging.getLogger(__name__)

DREMIO_DELIMITER = '.'


def _remove_source_from_path(table_schema: str) -> str:
    """
    Return dremio path without the source id at the start
    """
    if DREMIO_DELIMITER in table_schema:
        _, schema_path = table_schema.split(DREMIO_DELIMITER, 1)
        return schema_path
    else:
        raise MissingSchemaException('Unable to remove source from table path as there seems to be no schema')


def load_scanned_tables(raw_columns: Iterable[Dict], api_version: str) -> List[PanoModel]:
    """
    Load result of metadata table columns scan into Model
    """
    models = []
    columns_grouped = itertools.groupby(raw_columns, operator.itemgetter('table_schema', 'table_name'))

    for (table_schema, table_name), columns in columns_grouped:
        fields = []
        schema_path = _remove_source_from_path(table_schema)
        table_path = '.'.join([schema_path, table_name])
        table_file_name = pydash.slugify(table_path, separator="_").lower()

        for col in columns:
            data_type = col['data_type']
            column_name = col['column_name']
            field_path = slug_string('.'.join([table_path, column_name]))
            fields.append(dict(data_type=data_type, transformation=column_name, field_map=[field_path],))

        models.append(
            PanoModel.from_dict(
                dict(
                    table_file_name=table_file_name,
                    data_source=table_path,
                    fields=fields,
                    joins=[],
                    identifiers=[],
                    api_version=api_version,
                )
            )
        )

    return models
