import itertools
import logging
import operator
from typing import Dict, Iterable, List

from panoramic.cli.pano_model import PanoModel
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
            field_path = slug_string('.'.join([full_table_path, column_name]))
            fields.append(dict(data_type=data_type, transformation=column_name, field_map=[field_path],))

        models.append(
            PanoModel.from_dict(
                dict(model_name=model_name, data_source=full_table_path, fields=fields, joins=[], identifiers=[],)
            )
        )

    return models
