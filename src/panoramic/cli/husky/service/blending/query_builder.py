import json
from logging import getLogger
from typing import Optional

from panoramic.cli.husky.service.blending.blending_taxon_manager import (
    BlendingTaxonManager,
)
from panoramic.cli.husky.service.blending.comparison_request_builder import (
    ComparisonRequestBuilder,
)
from panoramic.cli.husky.service.blending.dataframe_joins import (
    blend_dataframes,
    left_join_dataframes,
)
from panoramic.cli.husky.service.blending.dimension_phase_builder import (
    DimensionPhaseBuilder,
)
from panoramic.cli.husky.service.blending.exceptions import InvalidComparisonRequest
from panoramic.cli.husky.service.blending.features.override_mapping.manager import (
    OverrideMappingManager,
)
from panoramic.cli.husky.service.blending.metric_phase_builder import MetricPhaseBuilder
from panoramic.cli.husky.service.blending.preprocessing import preprocess_request
from panoramic.cli.husky.service.blending.projection_builder import ProjectionBuilder
from panoramic.cli.husky.service.context import HuskyQueryContext
from panoramic.cli.husky.service.query_builder import QueryBuilder as MainQueryBuilder
from panoramic.cli.husky.service.types.api_data_request_types import BlendingDataRequest
from panoramic.cli.husky.service.types.types import (
    BlendingQueryInfo,
    Dataframe,
    QueryInfo,
)

logger = getLogger(__name__)


class QueryBuilder:
    """
    Class for processing blending data requests.
    """

    @classmethod
    def _build_data_blend_query(
        cls,
        ctx: HuskyQueryContext,
        taxon_manager: BlendingTaxonManager,
        config_arg: BlendingDataRequest,
        query_info: BlendingQueryInfo,
    ) -> Dataframe:
        """
        Builds subquery for each subrequest and then blends them all into one dataframe.
        :param ctx: Husky query context
        """
        dataframes = []
        request = BlendingDataRequest(config_arg.to_native())  # Clone, coz we will be modifying subqueries
        for subrequest in request.data_subrequests:
            # add comparison taxons to data subrequest taxons
            subrequest.taxons = taxon_manager.get_subrequest_taxons(subrequest)
            sub_query_info = QueryInfo(
                {
                    'used_raw_taxons': subrequest.taxons,
                }
            )
            query_info.subrequests_info.append(sub_query_info)

            # Build query for subrequest and add it to the list
            data_source = subrequest.properties.data_source
            dimension_templates = taxon_manager.plan.data_source_formula_templates[data_source]
            filter_templates = taxon_manager.plan.data_source_filter_templates[data_source]
            df = MainQueryBuilder.build_query(
                ctx,
                subrequest.to_internal_model(),
                sub_query_info,
                taxon_manager.used_taxons,
                dimension_templates,
                filter_templates=filter_templates,
                allowed_physical_data_sources=set(request.physical_data_sources)
                if request.physical_data_sources
                else None,
            )
            dataframes.append(df)

        return blend_dataframes(ctx, dataframes, taxon_manager.plan.data_source_formula_templates)

    @classmethod
    def _preprocess_request(cls, request: BlendingDataRequest):
        """
        Helper fn that is moving some values around, to be backward compatible.
        """
        preprocess_request(request)

    @classmethod
    def build_query(
        cls, ctx: HuskyQueryContext, req: BlendingDataRequest, query_info: Optional[BlendingQueryInfo] = None
    ) -> Dataframe:
        """
        Builds blended query

        Adding suggested comparison taxons (if desired, but missing)
        - attempt to use provided rules and generate the query using all taxons from the matched rule
        - if it fails, fall back to using only taxon Data Source as comparison taxon

        :param ctx: Husky query context
        :param req: Original request from API
        :param query_info: Optional query info structure

        :return: Generated blended data frame
        """
        query_info = query_info or BlendingQueryInfo.create(req, ctx)

        # Before we touch the request, let's log exactly how client sent it.
        query_info.original_request_str = json.dumps(req.to_primitive())

        return cls._build_query(ctx, req, query_info)

    @classmethod
    def _build_query(
        cls, ctx: HuskyQueryContext, request: BlendingDataRequest, query_info: BlendingQueryInfo
    ) -> Dataframe:
        """
        1. preprocess the request to make it backward compatible
        2. blend data df from subrequests
        3. build comparison df from subrequests (suggest comparison taxons, if needed and possible)
        4. data left join comparisons
        5. group by request dimensions
        """
        cls._preprocess_request(request)

        taxon_manager = BlendingTaxonManager(request)
        taxon_manager.load_all_used_taxons(ctx)

        company_id = request.data_subrequests[0].scope.company_id

        override_mapping_manager = OverrideMappingManager.initialize(
            company_id, taxon_manager.plan.override_mappings, taxon_manager.plan.comparison_override_mappings
        )

        # Build data df
        data_df = QueryBuilder._build_data_blend_query(ctx, taxon_manager, request, query_info)
        data_df = DimensionPhaseBuilder.calculate_dataframe(
            taxon_manager.plan.dimension_formulas,
            override_mapping_manager.override_mapping_tel_data,
            override_mapping_manager.cte_map,
            data_df,
        )

        blended_df = None
        if request.comparison is None:
            blended_df = data_df
        else:
            if request.comparison is None or request.comparison.taxons is None:
                raise InvalidComparisonRequest()

            # Build comparison df and join to data df
            comparison_df = ComparisonRequestBuilder.build_comparison_query(
                ctx,
                request,
                taxon_manager,
                override_mapping_manager,
                query_info,
                set(request.physical_data_sources) if request.physical_data_sources else None,
            )
            if comparison_df and comparison_df.slug_to_column:
                blended_df = left_join_dataframes(ctx, data_df, comparison_df, taxon_manager.plan)
            else:
                # No taxons in comparison df, thus no point in joining, just return the data df.
                blended_df = data_df

        # get all taxons to be returned
        return_taxons = taxon_manager.get_return_taxons()

        calculated_df = MetricPhaseBuilder(taxon_manager).calculate_dataframe(
            ctx,
            blended_df,
            blended_df.used_physical_data_sources,
            request.grouping_sets,
            filter_clause=request.filters,
        )

        # Project them to final df
        projected_df = ProjectionBuilder.project_dataframe(
            calculated_df,
            return_taxons,
            calculated_df.used_physical_data_sources,
            request.order_by,
            request.limit,
        )

        return projected_df

    @staticmethod
    def validate_data_request(ctx: HuskyQueryContext, data_request: BlendingDataRequest) -> Dataframe:
        query_info = BlendingQueryInfo.create(data_request, ctx)
        return QueryBuilder.build_query(ctx, data_request, query_info)
