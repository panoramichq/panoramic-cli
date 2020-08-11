import itertools
import logging
import operator
from typing import Dict, Iterable, List

from panoramic.cli.pano_model import PanoModel, PanoModelField

logger = logging.getLogger(__name__)


def load_scanned_tables(raw_columns: Iterable[Dict]) -> List[PanoModel]:
    """
    Load result of metadata table columns scan into Model
    """
    models = []
    columns_grouped = itertools.groupby(raw_columns, operator.itemgetter('fully_qualified_object_name', 'model_name'))

    for (fully_qualified_obj_name, model_name), columns in columns_grouped:
        fields = []

        for col in columns:
            fields.append(
                PanoModelField(
                    uid=col['uid'],
                    data_type=col['data_type'],
                    data_reference=col['data_reference'],
                    field_map=col['field_map'],
                )
            )

        models.append(
            PanoModel(
                model_name=model_name, data_source=fully_qualified_obj_name, fields=fields, joins=[], identifiers=[],
            )
        )

    return models
