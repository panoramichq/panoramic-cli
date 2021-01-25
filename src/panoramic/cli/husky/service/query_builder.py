import logging
from typing import List, Optional, Set

from panoramic.cli.husky.core.sql_alchemy_util import compile_query
from panoramic.cli.husky.core.tel.result import TaxonToTemplate
from panoramic.cli.husky.core.tel.sql_formula import SqlFormulaTemplate
from panoramic.cli.husky.service.context import HuskyQueryContext
from panoramic.cli.husky.service.graph_builder.component import (
    GraphBuilder,
    MultipleDataSources,
)
from panoramic.cli.husky.service.model_retriever.component import ModelRetriever
from panoramic.cli.husky.service.projection_builder.component import ProjectionBuilder
from panoramic.cli.husky.service.select_builder.component import SelectBuilder
from panoramic.cli.husky.service.types.api_data_request_types import InternalDataRequest
from panoramic.cli.husky.service.types.types import (
    Dataframe,
    QueryDefinition,
    QueryInfo,
)
from panoramic.cli.husky.service.utils.simple_taxon_manager import SimpleTaxonManager
from panoramic.cli.husky.service.utils.taxon_slug_expression import TaxonMap

logger = logging.getLogger(__name__)


class QueryBuilder:
    @staticmethod
    def build_query(
        ctx: HuskyQueryContext,
        subrequest: InternalDataRequest,
        query_info: QueryInfo,
        preloaded_taxons: TaxonMap,
        dimension_templates: Optional[List[SqlFormulaTemplate]] = None,
        filter_templates: Optional[TaxonToTemplate] = None,
        allowed_physical_data_sources: Optional[Set[str]] = None,
    ) -> Dataframe:
        """
        Returns Query and Taxons obtained in it
        :param dimension_templates Sql column templates to select
        :param filter_templates Filter temples keyed by taxon slug, referenced from scope or preagg filters.
        """
        dimension_templates = dimension_templates or []
        filter_templates = filter_templates or dict()
        # Fetch Taxons
        simple_taxon_manager = SimpleTaxonManager.initialize(
            subrequest, dimension_templates, filter_templates, preloaded_taxons
        )

        data_sources = set(subrequest.properties.data_sources)
        if len(subrequest.properties.data_sources) != 1:
            # Joining across data sources is more complex and not implemented yet.
            raise MultipleDataSources(data_sources)
        data_source = subrequest.properties.data_sources[0]

        models = ModelRetriever.load_models(
            data_sources, subrequest.scope, subrequest.properties.model_name, allowed_physical_data_sources
        )
        physical_data_sources = {model.physical_data_source for model in models}

        # Build Graph
        graph = GraphBuilder.create_with_models(models)

        # Create Select Query
        select_query, taxon_model_info_map, effectively_used_models = SelectBuilder(
            ctx,
            subrequest.scope,
            simple_taxon_manager.graph_select_taxons,
            simple_taxon_manager.projection_taxons,
            graph,
            data_source,
            subrequest.preaggregation_filters,
            dimension_templates,
            filter_templates,
        ).get_query()

        query_info.definition = QueryDefinition({'effectively_used_models': effectively_used_models})

        logger.debug('Select Query: %s', compile_query(select_query, ctx.dialect))

        # Create Projection Query
        final_dataframe = ProjectionBuilder.query(
            select_query,
            taxon_model_info_map,
            simple_taxon_manager.projection_taxons,
            subrequest.properties.data_source,
            subrequest.order_by,
            subrequest.limit,
            subrequest.offset,
            physical_data_sources,
            dimension_templates,
        )

        logger.debug('Projection Query: %s', compile_query(final_dataframe.query, ctx.dialect))
        return final_dataframe
