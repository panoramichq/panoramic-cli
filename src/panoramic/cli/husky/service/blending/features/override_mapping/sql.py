from typing import Dict

from sqlalchemy import and_, literal, literal_column, or_, union_all
from sqlalchemy.sql import Select

from panoramic.cli.husky.core.sql_alchemy_util import safe_identifier
from panoramic.cli.husky.core.taxonomy.override_mapping.models import OverrideMapping
from panoramic.cli.husky.core.taxonomy.override_mapping.types import (
    OverrideMappingSlug,
    OverrideMappingTelData,
)


class OverrideMappingSql:
    """
    Rendering override mappings to SQL
    """

    ORIGINAL_COLUMN_NAME = 'original'
    """Column name representing the original value"""

    CHANGED_COLUMN_NAME = 'changed'
    """Column name representing the changed value"""

    PANO_NULL = '--PANO-NULL--'
    """Temporary replacement for NULL value in CTE"""

    @staticmethod
    def generate_cte_name(slug: str) -> str:
        """Generated name for CTE representing override mapping with the slug"""
        return safe_identifier(f'__om_{slug}')

    @staticmethod
    def generate_identifier(column_name: str, slug: str, include_unknown_values: bool) -> str:
        """Generate SQL identifier for the mapping"""
        return safe_identifier(f'__om_{column_name}_{slug}_{str(include_unknown_values)}')

    @classmethod
    def render_direct_mapping(cls, mapping: OverrideMapping) -> Select:
        """Renders CTE for direct mapping as union of all values"""

        selects = []
        for original, changed in mapping.definition:
            # using "literal" instead of "literal_column" here to force SQLAlchemy to bind constants as params (safe)
            if original is None:
                original_column = literal_column('CAST(NULL AS VARCHAR)')
            else:
                original_column = literal(original)

            if changed is None:
                changed_column = literal(cls.PANO_NULL)
            else:
                changed_column = literal(changed)

            selects.append(
                Select([original_column.label(cls.ORIGINAL_COLUMN_NAME), changed_column.label(cls.CHANGED_COLUMN_NAME)])
            )

        return union_all(*selects)

    @classmethod
    def insert_cte_joins(
        cls,
        original_query: Select,
        override_mappings: OverrideMappingTelData,
        cte_map: Dict[OverrideMappingSlug, Select],
    ) -> Select:
        select_from_query = original_query
        if override_mappings:
            for om_data in sorted(override_mappings):

                identifier = cls.generate_identifier(
                    om_data.column, om_data.override_mapping_slug, om_data.include_missing_values
                )

                aliased_cte = cte_map[identifier]

                join_condition = or_(
                    aliased_cte.columns[cls.ORIGINAL_COLUMN_NAME] == literal_column(om_data.column),
                    and_(
                        aliased_cte.columns[cls.ORIGINAL_COLUMN_NAME].is_(None),
                        literal_column(om_data.column).is_(None),
                    ),
                )

                select_from_query = select_from_query.join(
                    aliased_cte, join_condition, isouter=om_data.include_missing_values
                )

        return select_from_query
