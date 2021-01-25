from typing import Iterable, List

from panoramic.cli.husky.core.tel.result import TaxonToTemplate
from panoramic.cli.husky.core.tel.sql_formula import SqlFormulaTemplate
from panoramic.cli.husky.service.types.api_data_request_types import InternalDataRequest
from panoramic.cli.husky.service.utils.taxon_slug_expression import (
    SlugExprTaxonMap,
    TaxonExpressionStr,
    TaxonMap,
    TaxonSlugExpression,
)


class SimpleTaxonManager:
    def __init__(self, graph_select_taxons: SlugExprTaxonMap, projection_taxons: SlugExprTaxonMap):
        self.graph_select_taxons = graph_select_taxons
        self.projection_taxons = projection_taxons

    @classmethod
    def initialize(
        cls,
        request: InternalDataRequest,
        dimension_templates: List[SqlFormulaTemplate],
        filter_templates: TaxonToTemplate,
        preloaded_taxons: TaxonMap,
    ):
        projection_taxon_slugs = set(request.taxons)
        order_by_taxon_slugs = {clause.taxon for clause in (request.order_by or [])}

        projection_slugs = projection_taxon_slugs | order_by_taxon_slugs

        projection_taxons = cls._get_expr_taxon_map(projection_slugs, preloaded_taxons)

        slugs_to_get = set()
        for template in filter_templates.values():
            slugs_to_get.update(template.used_taxons)
        for template in dimension_templates:
            slugs_to_get.update(template.used_taxons)
        graph_select_taxons = cls._get_expr_taxon_map(slugs_to_get, preloaded_taxons)

        # add the projection taxons from request
        graph_select_taxons.update(projection_taxons)

        # add taxons from aggregation definitions
        aggregation_taxon_slugs = set()
        for taxon in graph_select_taxons.values():
            if taxon.tel_metadata and taxon.tel_metadata.aggregation_definition:
                aggregation_taxon_slugs |= taxon.tel_metadata.aggregation_definition.used_taxon_slugs()

        agg_taxons = cls._get_expr_taxon_map(aggregation_taxon_slugs, preloaded_taxons)
        graph_select_taxons.update(agg_taxons)

        return cls(graph_select_taxons, projection_taxons)

    @classmethod
    def _get_expr_taxon_map(cls, slugs: Iterable[str], preloaded_taxons: TaxonMap) -> SlugExprTaxonMap:
        taxons = [preloaded_taxons[TaxonExpressionStr(slug)] for slug in slugs]
        expr_taxon_map = {TaxonSlugExpression(taxon.slug): taxon for taxon in taxons if taxon.calculation is None}

        return expr_taxon_map
