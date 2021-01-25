from typing import Dict, List, Optional, Set

import sqlalchemy
from sqlalchemy import cast, func, union_all
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import ColumnClause, literal_column

from panoramic.cli.husky.core.sql_alchemy_util import (
    AGGREGATION_TYPE_TO_SQLALCHEMY_FN,
    safe_identifier,
    safe_quote_identifier,
    sort_columns,
)
from panoramic.cli.husky.core.taxonomy.enums import AggregationType
from panoramic.cli.husky.service.blending.blending_taxon_manager import (
    BlendingTaxonManager,
)
from panoramic.cli.husky.service.context import HuskyQueryContext
from panoramic.cli.husky.service.filter_builder.component import FilterBuilder
from panoramic.cli.husky.service.filter_builder.filter_clauses import FilterClause
from panoramic.cli.husky.service.select_builder.taxon_model_info import TaxonModelInfo
from panoramic.cli.husky.service.types.api_data_request_types import GroupingSets
from panoramic.cli.husky.service.types.types import Dataframe, DataframeColumn

_PANORAMIC_GROUPINGSETS_NULL = 'PANORAMIC_GROUPINGSETS_NULL'


class MetricPhaseBuilder:

    AGGREGATION_FUNCTIONS_MAP: Dict[AggregationType, func.Function] = {
        AggregationType.sum: AGGREGATION_TYPE_TO_SQLALCHEMY_FN[AggregationType.sum],
        AggregationType.count_all: AGGREGATION_TYPE_TO_SQLALCHEMY_FN[AggregationType.sum],
        AggregationType.count_distinct: AGGREGATION_TYPE_TO_SQLALCHEMY_FN[AggregationType.sum],
    }
    """
    Map of specific aggregation functions to SQL functions in this phase
    """

    def __init__(self, taxon_manager: BlendingTaxonManager):
        self.taxon_manager = taxon_manager

    def calculate_dataframe(
        self,
        ctx: HuskyQueryContext,
        df: Dataframe,
        physical_data_sources: Set[str],
        grouping_sets: Optional[GroupingSets] = None,
        filter_clause: Optional[FilterClause] = None,
    ) -> Dataframe:
        """
        Applies in this order:
        - pre aggregation logic
        - aggregation by group by or grouping sets
        - optional step of window function aggregation
        - after aggregation logic
        - filters. Filters are applied here to simplify the final query and apply filtering before filling date gaps.
        """
        pre_agg_columns = []  # Columns with applied aggregation function in aggregation step

        # Columns to select from window step - columns that are not removed and dont need window step
        select_from_window_step: List[ColumnClause] = []
        df_columns: List[DataframeColumn] = []  # Final df columns after all steps.
        group_columns = []
        final_columns: List[ColumnClause] = []
        for pre_formula in self.taxon_manager.plan.metric_pre:
            col = pre_formula.formula.label(pre_formula.label)
            aggregation_fn = self.AGGREGATION_FUNCTIONS_MAP.get(pre_formula.aggregation.type)

            if aggregation_fn:
                # we know the aggregation function so let's use it
                pre_agg_columns.append(aggregation_fn(col).label(pre_formula.label))
            else:
                # if no aggregation function is defined, then we simply group by this formula
                group_columns.append(col)

            select_from_window_step.append(col)

        # taxon slugs used in group by clause
        dimension_taxon_slugs = {group_column.name for group_column in group_columns}

        for post_formula, taxon in self.taxon_manager.plan.metric_post:
            post_formula_sql = post_formula.render_formula(ctx.dialect, dimension_taxon_slugs)
            col = post_formula_sql.label(taxon.slug_safe_sql_identifier)
            final_columns.append(col)
            df_columns.append(DataframeColumn(taxon.slug_expr, taxon))

        # Aggregation query with column logic. This is the first aggregation step, regular group by
        # or a common table expression with multiple group by statements in case of grouping sets.
        pre_query = self._add_aggregation(df.query, pre_agg_columns, group_columns, grouping_sets)

        # Post aggregation logic
        post_query = Select(columns=sort_columns(final_columns)).select_from(pre_query)

        slug_to_column = Dataframe.dataframe_columns_to_map(df_columns)
        if filter_clause:
            taxon_model_info = {
                str(slug): TaxonModelInfo(safe_quote_identifier(slug, ctx.dialect)) for slug in slug_to_column.keys()
            }
            post_query = FilterBuilder.augment_query(ctx, post_query, taxon_model_info, filter_clause)

        return Dataframe(post_query, slug_to_column, df.used_model_names, physical_data_sources)

    @classmethod
    def _add_aggregation(
        cls,
        inner_query: Select,
        aggregation_columns: List[ColumnClause],
        group_by_columns: List[ColumnClause],
        grouping_sets: Optional[GroupingSets] = None,
    ) -> Select:
        """
        Aggregates raw metric taxons. Groups by given dimension taxons or grouping sets.

        :param inner_query: Query to aggregate
        :param aggregation_columns: List of columns with applied aggregation function
        :param group_by_columns: List of columns to group by
        :param grouping_sets: Optional list of grouping sets to group by instead
        :return: Aggregated query
        """
        if grouping_sets:
            # Because we union _PANORAMIC_GROUPINGSETS_NULL with column that can be date(time) or number,
            # we must cast all group columns to text. Some DB engines fail when we do casting and grouping in one query,
            # thus here we need to stringify the group columns in the CTE, and not in the group by query just below...
            group_by_column_names = {col.name for col in group_by_columns}
            stringified_group_columns = []
            for col in inner_query.columns:
                if col.name in group_by_column_names:
                    stringified_group_columns.append(cast(col, sqlalchemy.VARCHAR).label(col.name))
                else:
                    stringified_group_columns.append(col)

            # common table expression reused by multiple grouping sets queries
            cte_query = (
                Select(columns=sort_columns(stringified_group_columns))
                .select_from(inner_query)
                .cte('__cte_grouping_sets')
            )
            grouping_sets_queries = []

            for grouping_set in grouping_sets:
                safe_grouping_set = [safe_identifier(col) for col in grouping_set]
                # dimensions in the grouping set, used to aggregate values with group by
                gs_group_columns = [col for col in group_by_columns if col.name in safe_grouping_set]
                # extra dimensions not in the grouping set, returned as custom null values
                gs_null_columns = [
                    literal_column(f"'{_PANORAMIC_GROUPINGSETS_NULL}'").label(col.name)
                    for col in group_by_columns
                    if col.name not in safe_grouping_set
                ]
                grouping_sets_queries.append(
                    Select(columns=sort_columns(gs_group_columns + gs_null_columns + aggregation_columns))
                    .select_from(cte_query)
                    .group_by(*sort_columns(gs_group_columns))
                )
            return union_all(*grouping_sets_queries)

        # If grouping sets are not defined, use all dimensions for grouping.
        return (
            Select(columns=sort_columns(group_by_columns + aggregation_columns))
            .select_from(inner_query)
            .group_by(*sort_columns(group_by_columns))
        )
