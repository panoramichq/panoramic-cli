from typing import Dict, Optional

from sqlalchemy.sql import Select

from panoramic.cli.husky.service.context import HuskyQueryContext
from panoramic.cli.husky.service.filter_builder.filter_clauses import FilterClause
from panoramic.cli.husky.service.select_builder.taxon_model_info import TaxonModelInfo


class FilterBuilder:
    """
    This module is responsible for adding all post-aggregation filter clauses to the query.
    """

    @classmethod
    def augment_query(
        cls,
        ctx: HuskyQueryContext,
        query: Select,
        taxon_model_info_map: Dict[str, TaxonModelInfo],
        filter_clause: Optional[FilterClause],
    ) -> Select:
        """
        Adds filters to the query

        :param ctx: Husky query runtime
        :param query: Original query
        :param taxon_model_info_map: Map of taxon slug expression to taxon model info
        :param filter_clause: Filter clauses

        :return: New query with all modifiers applied
        """
        if filter_clause:
            new_q = query.where(filter_clause.generate(ctx, query, taxon_model_info_map))
            return new_q
        else:
            return query
