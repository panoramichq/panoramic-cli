from panoramic.cli.field.client import Field
from panoramic.cli.pano_model import PanoField

NAMESPACE_DELIMITER = '|'


def map_field_from_remote(field: Field) -> PanoField:
    """Convert a remote field to local field."""
    if NAMESPACE_DELIMITER in field.slug:
        data_source, slug = field.slug.split(NAMESPACE_DELIMITER, 1)
    else:
        data_source = None
        slug = field.slug

    return PanoField(
        slug=slug,
        group=field.group,
        display_name=field.display_name,
        data_type=field.data_type,
        description=field.description,
        data_source=data_source,
        aggregation=field.aggregation,
        calculation=field.calculation,
    )


def map_field_from_local(field: PanoField) -> Field:
    """Convert a local field to a remote fields"""

    if field.data_source is not None:
        slug = field.data_source + NAMESPACE_DELIMITER + field.slug
    else:
        slug = field.slug

    return Field(
        slug=slug,
        group=field.group,
        display_name=field.display_name,
        data_type=field.data_type,
        description=field.description,
        data_source=field.data_source,
        aggregation=field.aggregation,
        calculation=field.calculation,
    )
