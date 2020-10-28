from typing import Optional, Tuple

from panoramic.cli.field.client import Field
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
