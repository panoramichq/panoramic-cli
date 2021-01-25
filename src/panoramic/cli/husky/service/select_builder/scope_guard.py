from typing import Dict

from sqlalchemy.sql import Select

from panoramic.cli.husky.service.context import HuskyQueryContext
from panoramic.cli.husky.service.filter_builder.component import FilterBuilder
from panoramic.cli.husky.service.select_builder.taxon_model_info import TaxonModelInfo
from panoramic.cli.husky.service.types.api_scope_types import Scope


class ScopeGuard:
    """
    Class for making sure that the scope filters are correctly set and applied.
    """

    @staticmethod
    def add_scope_row_filters(
        ctx: HuskyQueryContext,
        scope: Scope,
        query: Select,
        taxon_model_info_map: Dict[str, TaxonModelInfo],
    ) -> Select:
        if scope.all_filters:
            # Scope filters are defined, just use them.
            query = FilterBuilder.augment_query(ctx, query, taxon_model_info_map, scope.all_filters)
        return query
