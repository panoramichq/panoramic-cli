from typing import List, Optional

from panoramic.cli.husky.core.pydantic.model import (
    PydanticModel,
    non_empty_list,
    non_empty_str,
    reuse_validator,
)


class ValidateTaxonGetInput(PydanticModel):
    company_ids: List[str]
    taxon_slugs: Optional[List[str]]

    _validate_lists = reuse_validator('company_ids')(non_empty_list)
    _validate_strings = reuse_validator('company_ids', 'taxon_slugs', each_item=True)(non_empty_str)
