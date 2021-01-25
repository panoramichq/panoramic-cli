from typing import List, Optional

from pydantic import Field, validator

from panoramic.cli.husky.core.pydantic.model import PydanticModel, non_empty_str


class TransformRequest(PydanticModel):
    """
    Post-model Husky transformation request
    """

    requested_fields: List[str] = Field(..., min_items=1, alias='fields')
    """List of taxons (or taxonless expressions)"""

    filter: Optional[str]
    """Optional filter clause represented by TEL expression"""

    _validate_non_empty_fields = validator('requested_fields', each_item=True, allow_reuse=True)(non_empty_str)
