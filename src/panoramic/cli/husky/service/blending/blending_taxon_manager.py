from collections import defaultdict
from logging import getLogger
from typing import Dict, Iterable, List, Set, Tuple, cast

from panoramic.cli.husky.common.my_memoize import memoized_property
from panoramic.cli.husky.core.taxonomy.constants import TEL_EXPR_QUERY_PREFIX
from panoramic.cli.husky.core.taxonomy.getters import (
    fetch_all_used_taxons_map,
    get_used_raw_taxon_slugs_all,
)
from panoramic.cli.husky.core.taxonomy.models import (
    Taxon,
    taxon_slug_to_sql_friendly_slug,
)
from panoramic.cli.husky.core.taxonomy.override_mapping.models import OverrideMapping
from panoramic.cli.husky.core.taxonomy.override_mapping.types import OverrideMappingSlug
from panoramic.cli.husky.core.tel.exceptions import TelExpressionException
from panoramic.cli.husky.service.blending.exceptions import InvalidComparisonRequest
from panoramic.cli.husky.service.blending.tel_planner import TelPlan, TelPlanner
from panoramic.cli.husky.service.constants import TaxonSlugs
from panoramic.cli.husky.service.context import HuskyQueryContext
from panoramic.cli.husky.service.types.api_data_request_types import (
    ApiDataRequest,
    BlendingDataRequest,
    ComparisonConfig,
    InternalDataRequest,
)
from panoramic.cli.husky.service.types.api_scope_types import ComparisonScopeType
from panoramic.cli.husky.service.types.types import Dataframe, DataframeColumn
from panoramic.cli.husky.service.utils.exceptions import (
    HuskyInvalidTelException,
    InvalidRequest,
)
from panoramic.cli.husky.service.utils.taxon_slug_expression import (
    TaxonExpressionStr,
    TaxonMap,
    TaxonSlugExpression,
)

logger = getLogger(__name__)


class BlendingTaxonManager:
    """
    Set of helper functions for handling taxons on blending data request.
    """

    plan: TelPlan

    def __init__(self, request: BlendingDataRequest):
        self._request: BlendingDataRequest = request
        self.taxon_map: TaxonMap = dict()
        self.data_sources: Set[str] = set()
        """
        Set of data sources used in the request
        """
        self.taxon_to_ds: Dict[str, Set[str]] = self._create_taxon_to_data_sources_mapping()

        self.override_mappings_map: Dict[OverrideMappingSlug, OverrideMapping] = {}
        """Map of relevant override mappings"""

    def _create_taxon_to_data_sources_mapping(self):
        taxon_to_ds = defaultdict(set)
        for subrequest in self._request.data_subrequests:
            data_source = subrequest.properties.data_sources[0]
            taxons_in_subrequest = set(subrequest.taxons)
            if subrequest.order_by:
                taxons_in_subrequest.update(subrequest.order_by)
            for taxon in taxons_in_subrequest:
                taxon_to_ds[taxon_slug_to_sql_friendly_slug(taxon)].add(data_source)
        return taxon_to_ds

    def _get_raw_taxons(
        self, company_id: str, taxon_slugs: Iterable[TaxonExpressionStr], data_source: str
    ) -> Set[TaxonExpressionStr]:
        """
        Returns raw taxons needed by the taxons slugs for the given data source.
        """
        # We need to pass down all available data sources, so it can raise exception if required DS is missing
        raw_slugs = get_used_raw_taxon_slugs_all(company_id, taxon_slugs, self.data_sources, self.taxonless_map)
        raw_exprs = [TaxonSlugExpression(slug) for slug in raw_slugs]
        # Keep slugs only from given DS or without DS.
        raw_slugs_for_ds = {raw_expr.slug for raw_expr in raw_exprs if raw_expr.data_source in [None, data_source]}
        return raw_slugs_for_ds

    @property
    def taxonless_map(self) -> TaxonMap:
        """
        Map containing only taxonless slugs
        """
        return {slug: taxon for slug, taxon in self.taxon_map.items() if slug.startswith(TEL_EXPR_QUERY_PREFIX)}

    def get_subrequest_taxons(self, subrequest: ApiDataRequest) -> List[TaxonExpressionStr]:
        """
        Returns list of taxons to select for given subrequest dataframe.
        It replaces computed taxons with raw metric taxons and adds comparison taxons.
        """
        # TODO v1 - this will likely not work for tag taxons.. the return key is regular slug.
        # We should modify get_taxons_map, so the key slug is the original slug pass in the arg array.
        all_taxon_slugs = set(subrequest.taxons)
        # we also need to include taxon slugs from filters (in case they use taxons not specified in the final list)
        if subrequest.preaggregation_filters:
            all_taxon_slugs |= subrequest.preaggregation_filters.get_taxon_slugs()
        taxons = self._taxon_map_from_cache(all_taxon_slugs)
        raw_subrequest_taxon_slugs: Set[TaxonExpressionStr] = self._get_raw_taxons(
            subrequest.scope.company_id, taxons.keys(), subrequest.properties.data_source
        )

        raw_subrequest_taxon_slugs.update(self.plan.comparison_raw_taxon_slugs)

        if self._grouping_sets_taxon_slugs:
            # Add taxons from grouping sets.
            raw_grouping_slugs = self._get_raw_taxons(
                subrequest.scope.company_id, self._grouping_sets_taxon_slugs, subrequest.properties.data_source
            )
            raw_subrequest_taxon_slugs.update(raw_grouping_slugs)

        if self._request.filters:
            # Add taxons from postaggregation filters
            filter_taxon_slugs = self._request.filters.get_taxon_slugs()

            raw_post_agg_filters_slugs = self._get_raw_taxons(
                subrequest.scope.company_id, filter_taxon_slugs, subrequest.properties.data_source
            )
            raw_subrequest_taxon_slugs.update(raw_post_agg_filters_slugs)

        return list(raw_subrequest_taxon_slugs)

    def get_comparison_subrequest_raw_taxons(
        self, subrequest: InternalDataRequest, comparison: ComparisonConfig
    ) -> Set[TaxonExpressionStr]:
        """
        Returns taxons to get for given comparison subrequest.
        Should contain all raw metric taxons from the subrequest + comparison join taxons.
        """
        if comparison.taxons is None:
            raise InvalidComparisonRequest()

        subrequest_taxons = self._taxon_map_from_cache(subrequest.taxons)
        comparison_subrequest_taxon_slugs = set(self.plan.comparison_raw_taxon_slugs)

        comparable_slugs = [
            cast(TaxonExpressionStr, taxon.slug) for taxon in subrequest_taxons.values() if taxon.can_have_comparison
        ]
        raw_comparable_slugs: Set[TaxonExpressionStr] = set(
            get_used_raw_taxon_slugs_all(
                subrequest.scope.company_id,
                comparable_slugs,
                subrequest.properties.data_sources,
                self.taxonless_map,
            )
        )
        comparison_subrequest_taxon_slugs.update(raw_comparable_slugs)

        return comparison_subrequest_taxon_slugs

    @classmethod
    def create_comparison_taxon(cls, taxon: Taxon) -> Tuple[TaxonExpressionStr, Taxon]:
        """
        Converts regular taxon into comparison taxon, including the slug.
        """
        new_taxon = taxon.copy(deep=True)
        comparison_slug = TaxonExpressionStr(f'comparison@{taxon.slug}')
        new_taxon.slug = comparison_slug
        new_taxon.comparison_taxon_slug_origin = taxon.slug
        return TaxonExpressionStr(comparison_slug), new_taxon

    def get_aggregation_taxons(self, dataframe: Dataframe) -> Dict[TaxonExpressionStr, DataframeColumn]:
        """
        Returns taxons that should be in aggregated dataframe. That includes all dimensions from subrequest and all
        raw metrics from subrequests. It must not include dimensions that are used just for comparison.
        :param dataframe: Dataframe that will be aggregated
        :return:
        """
        # We start with all taxons on a dataframe...
        aggregation_taxons: Dict[TaxonExpressionStr, DataframeColumn] = dataframe.slug_to_column
        all_subrequest_taxons = set()

        for subrequest in self._request.data_subrequests:
            all_subrequest_taxons.update(subrequest.taxons)

        if self._request.comparison and self._request.comparison.taxons is not None:
            for comparison_taxon in self._request.comparison.taxons:
                # ... and remove taxons that are used ONLY for joining to comparisons.
                if comparison_taxon not in all_subrequest_taxons:
                    del aggregation_taxons[comparison_taxon]

        if self._request.grouping_sets:
            # If grouping sets are defined, we select and
            # group only by dimension taxons that are defined in the grouping sets.
            grouping_taxons = self._grouping_sets_taxon_slugs
            for k, v in list(aggregation_taxons.items()):
                if v.taxon.is_dimension and k not in grouping_taxons:
                    del aggregation_taxons[k]

        return aggregation_taxons

    def get_return_taxons(self) -> Dict[TaxonExpressionStr, Taxon]:
        """
        Returns taxons that should be returned to the user.
        This includes union of all taxons from the subrequests.
        For computed metric taxons, it adds comparison taxon.
        """
        return_taxons: Dict[TaxonExpressionStr, Taxon] = {}
        for subrequest in self._request.data_subrequests:
            subrequest_taxons = self._taxon_map_from_cache(subrequest.taxons)
            if self._request.comparison:
                # Only create comparison taxon if we are asking for comparison.
                comparison_taxons = {}
                for taxon in subrequest_taxons.values():
                    if taxon.can_have_comparison:
                        comparison_slug, comparison_taxon = self.create_comparison_taxon(taxon)
                        comparison_taxons[comparison_slug] = comparison_taxon
                return_taxons.update(comparison_taxons)
            return_taxons.update(cast(Dict[TaxonExpressionStr, Taxon], subrequest_taxons))
        if self._request.grouping_sets:
            # If grouping sets are defined, remove all other dimension taxons from final projection,
            # and only keep the grouping sets taxons.
            for k, v in list(return_taxons.items()):
                if v.is_dimension:
                    del return_taxons[k]
            grouping_taxon_slugs = self._grouping_sets_taxon_slugs
            grouping_taxons = self._taxon_map_from_cache(grouping_taxon_slugs)
            return_taxons.update(cast(Dict[TaxonExpressionStr, Taxon], grouping_taxons))
        return return_taxons

    def get_projection_taxons(self) -> TaxonMap:
        """
        Returns all taxons that are used inside Projection builder
        """
        all_slugs: Set[TaxonExpressionStr] = set()
        if self._request.filters:
            all_slugs.update(self._request.filters.get_taxon_slugs())
        all_slugs.update({TaxonExpressionStr(order.taxon) for order in self._request.order_by or []})
        projection_taxons_map = self._taxon_map_from_cache(all_slugs)
        projection_taxons_map.update(self.get_return_taxons())
        return projection_taxons_map

    def _taxon_map_from_cache(self, taxon_slugs: Iterable[TaxonExpressionStr]) -> TaxonMap:
        """
        Return taxons from memory cache.
        """
        result = dict()
        for slug in taxon_slugs:
            slug = taxon_slug_to_sql_friendly_slug(slug)
            taxon = self.taxon_map.get(slug)
            assert taxon, f'Taxon "{slug}" not in preloaded cache. Should never happen. Investigate.'
            result[slug] = taxon
        return result

    def get_taxon(self, taxon_slug: TaxonExpressionStr) -> Taxon:
        """
        Return taxons from memory cache.
        """
        return self.taxon_map[taxon_slug]

    @memoized_property
    def _grouping_sets_taxon_slugs(self) -> Set[TaxonExpressionStr]:
        """
        Returns a set of all slugs used inside grouping sets.
        """
        return {item for sublist in self._request.grouping_sets or [] for item in sublist or []}  # type: ignore

    def load_all_used_taxons(self, ctx: HuskyQueryContext):
        """
        Prepare map of all used taxons (even recursively) that we might need when processing the query.
        Mainly to minimize number of queries to postgres. Make sure any new field with taxons is used in this fn and
        its taxons are loaded.
        """
        company_id = self._request.data_subrequests[0].scope.company_id
        slugs_to_get: Set[TaxonExpressionStr] = set()
        slugs_to_get.update(self._grouping_sets_taxon_slugs)
        if self._request.filters:
            slugs_to_get.update(self._request.filters.get_taxon_slugs())
        if self._request.comparison:
            slugs_to_get.update(self._request.comparison.taxons or [])
            comparison_type = self._request.comparison.scope
            # Load the taxons we will need to filter the comparison dataframe
            if comparison_type == ComparisonScopeType.company:
                slugs_to_get.add(TaxonSlugs.COMPANY_ID)

        if self._request.order_by:
            slugs_to_get.update([cast(TaxonExpressionStr, order.taxon) for order in self._request.order_by])

        for subrequest in self._request.data_subrequests:
            self.data_sources.update(subrequest.properties.data_sources)
            slugs_to_get.update(subrequest.taxons or [])
            slugs_to_get.update(
                subrequest.scope.preaggregation_filters.get_taxon_slugs()
                if subrequest.scope.preaggregation_filters
                else []
            )
            slugs_to_get.update(
                subrequest.preaggregation_filters.get_taxon_slugs() if subrequest.preaggregation_filters else []
            )

        try:
            taxons_map = fetch_all_used_taxons_map(company_id, slugs_to_get, self.data_sources)
        except TelExpressionException as error:
            raise HuskyInvalidTelException(error, '__unknown_taxon__')

        self.taxon_map = taxons_map
        self._validate_grouping_sets_taxons()

        self.plan = TelPlanner.plan(ctx, self._request, self.get_projection_taxons(), self.taxon_map, self.taxon_to_ds)

    def get_used_raw_taxons(self) -> TaxonMap:
        """
        Returns taxons that are needed
        """
        return {key: taxon for key, taxon in self.taxon_map.items() if not taxon.is_computed_metric}

    @property
    def used_taxons(self) -> TaxonMap:
        assert self.taxon_map, 'Used taxon dict was not loaded yet'
        return self.taxon_map

    def get_taxon_data_sources(self, slug: str) -> Iterable[str]:
        return self.taxon_to_ds[slug]

    def _validate_grouping_sets_taxons(self):
        assert self.taxon_map, 'Used taxon dict was not loaded yet'
        metric_taxon_slugs = [
            taxon_slug for taxon_slug in self._grouping_sets_taxon_slugs if self.taxon_map[taxon_slug].is_metric
        ]
        if len(metric_taxon_slugs) > 0:
            raise InvalidRequest(
                'request.grouping_sets', f'Grouping sets cannot contain metric taxons: {metric_taxon_slugs}'
            )
