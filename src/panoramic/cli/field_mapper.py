from typing import Any, Dict, Optional, Tuple

from panoramic.cli.errors import MissingFieldFileError
from panoramic.cli.field.client import Aggregation, Field
from panoramic.cli.pano_model import PanoField

NAMESPACE_DELIMITER = '|'


def map_field_from_remote(field: Field) -> PanoField:
    """Convert a remote field to local field."""
    data_source, slug = map_field_slug_from_remote(field.slug)

    return PanoField(
        slug=slug,
        group=field.group,
        display_name=field.display_name,
        data_type=field.data_type,
        field_type=field.field_type,
        description=field.description,
        data_source=data_source,
        aggregation=field.aggregation,
        calculation=field.calculation,
        display_format=field.display_format,
    )


def map_field_slug_from_remote(slug: str) -> Tuple[Optional[str], str]:
    if NAMESPACE_DELIMITER in slug:
        data_source, mappped_slug = slug.split(NAMESPACE_DELIMITER, 1)
        return data_source, mappped_slug
    else:
        return None, slug


def map_field_slug_from_local(slug: str, data_source: Optional[str]) -> str:
    if data_source is not None:
        return data_source + NAMESPACE_DELIMITER + slug
    else:
        return slug


def map_field_from_local(field: PanoField) -> Field:
    """Convert a local field to a remote fields"""

    return Field(
        slug=map_field_slug_from_local(field.slug, field.data_source),
        group=field.group,
        display_name=field.display_name,
        data_type=field.data_type,
        field_type=field.field_type,
        description=field.description,
        data_source=field.data_source,
        aggregation=field.aggregation,
        calculation=field.calculation,
        display_format=field.display_format,
    )


def map_column_to_field(column: Dict[str, Any], is_identifier: bool = False) -> PanoField:
    aggregation = (
        Aggregation(type=column['aggregation_type'], params=None)
        if column.get('aggregation_type') is not None
        else None
    )
    field_type = 'dimension' if is_identifier else column['taxon_type']

    assert len(column['field_map']) == 1
    slug = column['field_map'][0]

    return PanoField(
        slug=slug,
        field_type=field_type,
        display_name=slug,
        group='CLI',
        data_type=column['validation_type'],
        aggregation=aggregation,
    )


def map_error_to_field(error: MissingFieldFileError) -> PanoField:
    return PanoField(
        slug=error.field_slug,
        field_type='TODO: add field_type',
        display_name=error.field_slug,
        group='CLI',
        data_type='TODO: add data_type',
    )
