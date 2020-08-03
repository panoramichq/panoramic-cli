import itertools
import logging
import operator
from typing import Dict, Iterable, List

from panoramic.cli.pano_model import PanoModel, PanoModelField
from panoramic.cli.util import slug_string

logger = logging.getLogger(__name__)


def load_scanned_tables(raw_columns: Iterable[Dict]) -> List[PanoModel]:
    """
    Load result of metadata table columns scan into Model
    """
    models = []
    columns_grouped = itertools.groupby(raw_columns, operator.itemgetter('table_schema', 'table_name'))

    for (table_schema, table_name), columns in columns_grouped:
        fields = []
        full_table_path = '.'.join([table_schema, table_name])
        model_name = slug_string(full_table_path)

        for col in columns:
            data_type = col['data_type']
            column_name = col['column_name']
            unique_field_path = slug_string('.'.join([full_table_path, column_name]))
            column_name_slug = slug_string(column_name)
            fields.append(
                PanoModelField(
                    uid=unique_field_path,
                    data_type=data_type,
                    data_reference=column_name,
                    field_map=[column_name_slug],
                )
            )

        models.append(
            PanoModel(model_name=model_name, data_source=full_table_path, fields=fields, joins=[], identifiers=[])
        )

    return models
