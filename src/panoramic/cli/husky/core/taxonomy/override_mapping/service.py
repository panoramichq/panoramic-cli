from typing import Collection, List, Optional

from panoramic.cli.husky.core.taxonomy.override_mapping.models import (
    OverrideMapping as PydanticOverrideMapping,
)


class OverrideMappingService:
    """Service layer for Taxonomy Override Mapping API."""

    @staticmethod
    def get_list(company_id: str, *, offset: int, limit: int) -> List[PydanticOverrideMapping]:
        return []

    @staticmethod
    def get_by_slug(slug: str, company_id: str) -> Optional[PydanticOverrideMapping]:
        return None

    @staticmethod
    def get_by_slugs_list(
        slugs: Collection[str], company_id: str, raise_on_missing: bool = False
    ) -> List[PydanticOverrideMapping]:
        return []
