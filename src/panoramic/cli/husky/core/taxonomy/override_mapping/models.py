from datetime import datetime
from typing import Optional

from pydantic import validator

from panoramic.cli.husky.core.pydantic.model import PydanticModel
from panoramic.cli.husky.core.taxonomy.override_mapping.enums import MappingSourceType
from panoramic.cli.husky.core.taxonomy.override_mapping.types import MappingDefinition
from panoramic.cli.husky.core.taxonomy.override_mapping.validators import (
    correct_length_mapping,
    unique_mapping,
)


class OverrideMapping(PydanticModel):
    """Structure holding override mapping definition"""

    slug: str
    name: str
    definition: MappingDefinition

    company_id: str
    source_type: MappingSourceType

    created_at: datetime
    created_by: str

    updated_at: Optional[datetime]
    updated_by: Optional[str]

    deleted_at: Optional[datetime]
    deleted_by: Optional[str]

    _definition = validator('definition', allow_reuse=True)(correct_length_mapping)
    _uniqueness_definition = validator('definition', allow_reuse=True)(unique_mapping)

    class Config:
        orm_mode = True


class OverrideMappingCreate(PydanticModel):
    name: str
    """Display name"""

    definition: MappingDefinition
    """Definition of the mapping - list of tuples"""

    _length_definition = validator('definition', allow_reuse=True)(correct_length_mapping)
    _uniqueness_definition = validator('definition', allow_reuse=True)(unique_mapping)


class OverrideMappingUpdate(PydanticModel):
    name: Optional[str]
    """Display name"""

    definition: Optional[MappingDefinition]
    """Definition of the mapping - list of tuples"""

    _length_definition = validator('definition', allow_reuse=True)(correct_length_mapping)
    _uniqueness_definition = validator('definition', allow_reuse=True)(unique_mapping)
