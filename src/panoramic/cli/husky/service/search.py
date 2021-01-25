import logging
from functools import reduce
from typing import List, Set

from panoramic.cli.husky.core.model.models import HuskyModel
from panoramic.cli.husky.core.taxonomy.getters import UsedTaxons
from panoramic.cli.husky.service.graph_builder.component import (
    GraphBuilder,
    MultipleDataSources,
)
from panoramic.cli.husky.service.model_retriever.component import ModelRetriever
from panoramic.cli.husky.service.select_builder.graph_search import GraphSearch
from panoramic.cli.husky.service.select_builder.query_joins import get_bfs_ordered
from panoramic.cli.husky.service.types.api_data_request_types import SearchRequest
from panoramic.cli.husky.service.utils.exceptions import HuskyException
from panoramic.cli.husky.service.utils.taxon_slug_expression import (
    TaxonExpressionStr,
    TaxonSlugExpression,
)

logger = logging.getLogger(__name__)


class HuskySearch:
    """
    Class handling searching for various objects (i.e. available taxons) using Husky and model graph
    """

    @staticmethod
    def _get_raw_taxon_slug_expressions(
        company_id: str, taxon_slugs: List[TaxonExpressionStr], data_sources: List[str]
    ) -> Set[TaxonSlugExpression]:
        """
        Fetches taxon slug expressions for all raw taxons
        """
        used_taxons = UsedTaxons.get_all_taxons(company_id, taxon_slugs, {}, data_sources)

        raw_taxon_slug_expressions = {
            TaxonSlugExpression(taxon.slug) for taxon in used_taxons.all_taxons.values() if not taxon.is_computed_metric
        }
        return raw_taxon_slug_expressions

    @classmethod
    def search_models(cls, search_request: SearchRequest) -> List[HuskyModel]:
        try:
            raw_taxon_slug_expressions = cls._get_raw_taxon_slug_expressions(
                search_request.scope.company_id, search_request.taxons, search_request.properties.data_sources
            )

            return ModelRetriever.load_models_by_taxons(
                {str(slug_expression.graph_slug) for slug_expression in raw_taxon_slug_expressions},
                set(search_request.properties.data_sources),
                search_request.scope,
                search_request.properties.model_name,
            )
        except HuskyException as error:
            error.set_scope(search_request.scope)

            raise error

    @classmethod
    def get_available_raw_taxons(cls, search_request: SearchRequest) -> Set[str]:
        # Fetch Taxons
        logger.debug('Starting get raw taxon slug expressions')
        raw_taxon_slug_expressions = cls._get_raw_taxon_slug_expressions(
            search_request.scope.company_id, search_request.taxons, search_request.properties.data_sources
        )
        logger.debug('Completed get raw taxon slug expressions')

        data_sources = set(search_request.properties.data_sources)
        if len(search_request.properties.data_sources) != 1:
            # Joining across data sources is more complex and not implemented yet.
            raise MultipleDataSources(data_sources)

        logger.debug('Starting load models')
        models = ModelRetriever.load_models(data_sources, search_request.scope, search_request.properties.model_name)
        logger.debug('Completed load models')

        # Build Graph
        logger.debug('Starting graph build')
        graph = GraphBuilder.create_with_models(models)
        logger.debug('Completed graph build')

        logger.debug('Starting graph search')
        query_joins = GraphSearch(
            graph.name_to_model, raw_taxon_slug_expressions, graph.model_graph
        ).find_all_full_join_trees()
        logger.debug('Completed graph search')

        available_raw_slugs: Set[str] = set()

        for query_join in query_joins:
            # Flatten the QueryJoins tree structure into a list. BFS is not required, but just reusing already existing fn.
            ordered_query_joins = get_bfs_ordered(query_join)

            available_raw_slugs = reduce(
                lambda acc, join: acc | join.model.taxons if join.model else acc,
                ordered_query_joins,
                available_raw_slugs,
            )
        return available_raw_slugs
