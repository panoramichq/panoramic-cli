import itertools
import logging
from collections import defaultdict
from typing import Dict, List, Sequence, Tuple

from panoramic.cli.errors import MissingFieldFileError
from panoramic.cli.husky.common.enum import EnumHelper
from panoramic.cli.husky.core.taxonomy.enums import (
    METRIC_VALIDATION_TYPES,
    TaxonTypeEnum,
    ValidationType,
)
from panoramic.cli.pano_model import Aggregation, PanoField, PanoModel

logger = logging.getLogger(__name__)


def _group_errors_by_column(
    errors: Sequence[MissingFieldFileError],
) -> Dict[Tuple[str, str], List[MissingFieldFileError]]:
    """Group errors by data_source+data_reference (table+column)."""
    errors_by_column: Dict[Tuple[str, str], List[MissingFieldFileError]] = defaultdict(list)
    for error in errors:
        errors_by_column[(error.model_name, error.data_reference)].append(error)
    return dict(errors_by_column)


def scan_fields_for_errors(
    errors: Sequence[MissingFieldFileError], loaded_models: Dict[str, PanoModel]
) -> List[PanoField]:
    fields: List[PanoField] = []
    errors_by_column = _group_errors_by_column(errors)
    fields.extend(
        map_error_to_field(error, loaded_models) for error in itertools.chain.from_iterable(errors_by_column.values())
    )

    return fields


def map_error_to_field(error: MissingFieldFileError, loaded_models: Dict[str, PanoModel]) -> PanoField:
    # try to find the field in scanned state
    model = loaded_models.get(error.model_name)
    data_type = ValidationType.text

    if model:
        # model with this field was scanned so let's try to find this field
        field = [model_field for model_field in model.fields if error.field_slug in model_field.field_map]

        if len(field) == 1:
            # exactly this field was scanned so let's determine its correct validation type
            field_data_type = EnumHelper.from_value_safe(ValidationType, field[0].data_type)
            if field_data_type:
                data_type = field_data_type

    field_type = TaxonTypeEnum.metric if data_type in METRIC_VALIDATION_TYPES else TaxonTypeEnum.dimension

    if field_type is TaxonTypeEnum.dimension:
        aggregation = Aggregation(type='group_by', params=None)
    else:
        aggregation = Aggregation(type='sum', params=None)

    return PanoField(
        slug=error.field_slug,
        field_type=field_type.value,
        display_name=error.field_slug,
        data_source=error.dataset_slug,
        group='CLI',
        data_type=data_type.value,
        aggregation=aggregation,
    )
