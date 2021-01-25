import itertools
import logging
from collections import defaultdict
from typing import Dict, List, Sequence, Tuple

from panoramic.cli.errors import MissingFieldFileError
from panoramic.cli.pano_model import Aggregation, PanoField

logger = logging.getLogger(__name__)


def _group_errors_by_column(
    errors: Sequence[MissingFieldFileError],
) -> Dict[Tuple[str, str], List[MissingFieldFileError]]:
    """Group errors by data_source+data_reference (table+column)."""
    errors_by_column: Dict[Tuple[str, str], List[MissingFieldFileError]] = defaultdict(list)
    for error in errors:
        errors_by_column[(error.data_source, error.data_reference)].append(error)
    return dict(errors_by_column)


def scan_fields_for_errors(errors: Sequence[MissingFieldFileError]) -> List[PanoField]:
    fields: List[PanoField] = []
    errors_by_column = _group_errors_by_column(errors)
    fields.extend(map_error_to_field(error) for error in itertools.chain.from_iterable(errors_by_column.values()))

    return fields


def map_error_to_field(error: MissingFieldFileError) -> PanoField:
    return PanoField(
        slug=error.field_slug,
        field_type='dimension',
        data_source=error.dataset_slug,
        display_name=error.field_slug,
        group='CLI',
        data_type='text',
        aggregation=Aggregation(type='group_by', params=None),
    )
