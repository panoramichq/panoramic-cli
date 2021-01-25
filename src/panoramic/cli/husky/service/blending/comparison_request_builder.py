from typing import Dict, Optional, Set

from sqlalchemy import column, literal_column, select

from panoramic.cli.husky.core.sql_alchemy_util import (
    quote_identifier,
    safe_identifier,
    sort_columns,
)
from panoramic.cli.husky.service.blending.blending_taxon_manager import (
    BlendingTaxonManager,
)
from panoramic.cli.husky.service.blending.dataframe_joins import blend_dataframes
from panoramic.cli.husky.service.blending.dimension_phase_builder import (
    DimensionPhaseBuilder,
)
from panoramic.cli.husky.service.blending.features.override_mapping.manager import (
    OverrideMappingManager,
)
from panoramic.cli.husky.service.blending.metric_phase_builder import MetricPhaseBuilder
from panoramic.cli.husky.service.blending.tel_planner import TelPlanner
from panoramic.cli.husky.service.context import HuskyQueryContext
from panoramic.cli.husky.service.filter_builder.enums import (
    FilterClauseType,
    SimpleFilterOperator,
)
from panoramic.cli.husky.service.filter_builder.filter_clauses import (
    TaxonValueFilterClause,
)
from panoramic.cli.husky.service.query_builder import QueryBuilder
from panoramic.cli.husky.service.select_builder.exceptions import (
    UnsupportedAggregationType,
)
from panoramic.cli.husky.service.types.api_data_request_types import (
    ApiDataRequest,
    BlendingDataRequest,
    ComparisonConfig,
    InternalDataRequest,
)
from panoramic.cli.husky.service.types.api_scope_types import ComparisonScopeType
from panoramic.cli.husky.service.types.types import (
    BlendingQueryInfo,
    Dataframe,
    DataframeColumn,
    QueryInfo,
)
from panoramic.cli.husky.service.utils.taxon_slug_expression import TaxonExpressionStr


class ComparisonRequestBuilder:
    """
    Helper class for building Husky comparison subrequests.
    """

    @classmethod
    def _build_comparison_subrequest(
        cls, original_subrequest: ApiDataRequest, comparison: ComparisonConfig, taxon_manager: BlendingTaxonManager
    ) -> InternalDataRequest:
        subrequest: InternalDataRequest = original_subrequest.to_internal_model()

        # Reset all filters. Getting comparison can only be filtered by project filters or company id.
        subrequest.preaggregation_filters = None

        # Reset limit and order by. Does not make sense for comparison.
        subrequest.limit = None
        subrequest.order_by = []

        # Get taxon slugs we need for comparison subrequest.
        subrequest.taxons = sorted(list(taxon_manager.get_comparison_subrequest_raw_taxons(subrequest, comparison)))

        if comparison.scope == ComparisonScopeType.company:
            # If company scope, we add a filter on the company id and remove project filters and accounts
            # Eventually, we could fetch list of all accounts under a company and filter on that, since that will
            # probably be faster.
            subrequest.scope.preaggregation_filters = TaxonValueFilterClause(
                {
                    'type': FilterClauseType.TAXON_VALUE.value,
                    'taxon': 'company_id',
                    'operator': SimpleFilterOperator.EQ.value,
                    'value': subrequest.scope.company_id,
                }
            )

        return subrequest

    @classmethod
    def _build_comparison_blend_query(
        cls,
        ctx: HuskyQueryContext,
        config_arg: BlendingDataRequest,
        taxon_manager: BlendingTaxonManager,
        query_info: BlendingQueryInfo,
        allowed_physical_data_sources: Optional[Set[str]] = None,
    ) -> Optional[Dataframe]:
        """
        Builds comparison query for each subrequest and then blends them all into one comparison dataframe.
        """
        dataframes = []
        config = BlendingDataRequest(config_arg.to_native())  # Clone, coz we will be modifying subqueries
        assert config.comparison, 'Comparison must be defined when trying to build comparison query..'
        comparison: ComparisonConfig = config.comparison
        for _subrequest in config.data_subrequests:
            subrequest = cls._build_comparison_subrequest(_subrequest, comparison, taxon_manager)
            data_source = subrequest.properties.data_source

            # if no comparison taxons were found for this subrequest, skip creating comparison query for it as well
            if len(subrequest.taxons) == 0:
                continue

            bm_sub_query_info = QueryInfo.create(subrequest)
            query_info.comparison_subrequests_info.append(bm_sub_query_info)
            # Build comparison dataframe and add it to a list.
            # TODO pass down TelPlan for comparisons
            # ComparisonRequestBuilder might have added filters (typically for company id project id)
            # Me create new filter templates for this comparison subrequest.
            filter_templates = TelPlanner.get_preaggregation_filter_templates(
                ctx,
                [subrequest.preaggregation_filters, subrequest.scope.preaggregation_filters],
                taxon_manager.taxon_map,
                data_source,
            )

            dataframes.append(
                QueryBuilder.build_query(
                    ctx,
                    subrequest,
                    bm_sub_query_info,
                    taxon_manager.used_taxons,
                    dimension_templates=taxon_manager.plan.comparison_data_source_formula_templates[data_source],
                    filter_templates=filter_templates,
                    allowed_physical_data_sources=allowed_physical_data_sources,
                )
            )

        # if no comparison subrequests were created, there is no need to blend data frames
        if len(dataframes) == 0:
            return None

        # Blend all comparison dataframes into one
        # TODO pass down TelPlan for comparisons
        data_source_formula_templates = taxon_manager.plan.comparison_data_source_formula_templates
        dataframe = blend_dataframes(ctx, dataframes, data_source_formula_templates)

        # Prefix all comparison metric columns with 'comparison@' and create comparison taxon for it.
        query = dataframe.query
        final_columns = []
        aliased_taxon_by_slug: Dict[TaxonExpressionStr, DataframeColumn] = dict()
        for slug, df_column in dataframe.slug_to_column.items():
            # Alias metrics with comparison@ prefix, and select dimensions..
            if df_column.taxon.is_dimension:
                new_taxon = df_column.taxon.copy(deep=True)
                new_slug = TaxonExpressionStr(f'{slug}')
            else:
                new_slug, new_taxon = BlendingTaxonManager.create_comparison_taxon(df_column.taxon)

            final_columns.append(query.c[safe_identifier(slug)].label(new_taxon.slug_safe_sql_identifier))
            aliased_taxon_by_slug[new_slug] = DataframeColumn(new_slug, new_taxon, df_column.quantity_type)
        for pre_formulas in data_source_formula_templates.values():
            # and also select the dim columns from dim templates.
            for pre_formula in pre_formulas:
                final_columns.append(literal_column(quote_identifier(pre_formula.label, ctx.dialect)))
        renamed_cols_query = select(sort_columns(final_columns)).select_from(dataframe.query)
        return Dataframe(
            renamed_cols_query, aliased_taxon_by_slug, dataframe.used_model_names, dataframe.used_physical_data_sources
        )

    @classmethod
    def build_comparison_query(
        cls,
        ctx: HuskyQueryContext,
        config_arg: BlendingDataRequest,
        taxon_manager: BlendingTaxonManager,
        override_mapping_manager: OverrideMappingManager,
        query_info: BlendingQueryInfo,
        allowed_physical_data_sources: Optional[Set[str]] = None,
    ) -> Optional[Dataframe]:
        comp_df = cls._build_comparison_blend_query(
            ctx, config_arg, taxon_manager, query_info, allowed_physical_data_sources=allowed_physical_data_sources
        )
        if comp_df is None or len(taxon_manager.plan.comparison_dimension_formulas) == 0:
            # There are no comparison dim formulas, means the rows are already grouped correctly
            return comp_df

        comp_df = DimensionPhaseBuilder.calculate_dataframe(
            taxon_manager.plan.comparison_dimension_formulas,
            override_mapping_manager.comparison_override_mapping_tel_data,
            override_mapping_manager.cte_map,
            comp_df,
        )

        # After dimension join, there could have been a merge (coalesce). We need to group them by the merged column
        # once more, to keep single row per dimension.. otherwise we will get row fanout when left joining with
        # data dataframe
        group_by_cols = []
        selectors = []
        for dim_formula in taxon_manager.plan.comparison_dimension_formulas:
            group_by_cols.append(column(dim_formula.label))
        for df_column in comp_df.slug_to_column.values():
            taxon = df_column.taxon
            col = column(df_column.name)
            if taxon.is_dimension:
                group_by_cols.append(col)
            else:
                agg_type = taxon.tel_metadata_aggregation_type
                agg_fn = None
                if agg_type:
                    agg_fn = MetricPhaseBuilder.AGGREGATION_FUNCTIONS_MAP.get(agg_type)

                if agg_fn is None:
                    raise UnsupportedAggregationType(taxon)
                col = agg_fn(col).label(df_column.name)
                selectors.append(col)
        selectors.extend(group_by_cols)
        query = select(sort_columns(selectors)).select_from(comp_df.query).group_by(*group_by_cols)

        return Dataframe(query, comp_df.slug_to_column, comp_df.used_model_names, comp_df.used_physical_data_sources)
