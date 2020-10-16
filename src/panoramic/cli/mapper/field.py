from panoramic.cli.field.client import Field
from panoramic.cli.pano_model import PanoField

NAMESPACE_DELIMITER = '|'


def map_field_from_remote(field: Field) -> PanoField:
    """Convert a remote field to local field."""
    # company_slug = ''
    #
    # return PanoField(slug=field.slug, company_slug)


def map_field_from_local(field: PanoField) -> Field:
    """Convert a local field to a remote fields"""

    return Field(slug=field.slug)
