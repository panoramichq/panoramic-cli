from typing import Dict, Iterable, List, Optional, Set, Tuple, cast

from panoramic.cli.husky.common.exception_enums import ExceptionSeverity
from panoramic.cli.husky.core.taxonomy.exceptions import TaxonsNotFound
from panoramic.cli.husky.core.taxonomy.getters import Taxonomy
from panoramic.cli.husky.core.taxonomy.models import Taxon
from panoramic.cli.husky.core.tel.exceptions import TelExpressionException
from panoramic.cli.husky.core.tel.tel import Tel
from panoramic.cli.husky.service.types.api_data_request_types import InternalDataRequest
from panoramic.cli.husky.service.types.api_scope_types import Scope
from panoramic.cli.husky.service.utils.exceptions import HuskyInvalidTelException
from panoramic.cli.husky.service.utils.taxon_slug_expression import (
    TaxonExpressionStr,
    TaxonMap,
    TaxonSlugExpression,
)


class QueryTaxonManager:
    """
    This component handles management of taxons for requested query. It is able to provide
    all necessary
    """

    def __init__(self, projection_taxon_slugs: List[str], projection_taxons: Dict[str, Taxon]):
        """
        Constructor

        :param projection_taxon_slugs: List of requested taxon slugs
        :param taxons: All taxon definitions handled by the manager
        """

        self._loaded_taxons: Dict[str, Taxon] = {**projection_taxons}

        self._projection_taxons: List[Tuple[TaxonExpressionStr, Taxon]] = self.get_projection_taxons(
            projection_taxon_slugs, projection_taxons
        )

        self._selected_raw_taxons = self.get_raw_taxons(projection_taxons)

        self._raw_taxons: Dict[TaxonSlugExpression, Taxon] = self.get_raw_taxons(self._loaded_taxons)

    @classmethod
    def _get_taxons(cls, taxons: Dict[str, Taxon]) -> List[Tuple[TaxonSlugExpression, Taxon]]:
        slug_expression_taxons = [(TaxonSlugExpression(taxon.slug), taxon) for taxon in taxons.values()]

        return slug_expression_taxons

    @classmethod
    def get_raw_taxons(cls, taxons: Dict[str, Taxon]) -> Dict[TaxonSlugExpression, Taxon]:
        return {
            taxon_slug_expression: taxon
            for taxon_slug_expression, taxon in cls._get_taxons(taxons)
            if not taxon.calculation
        }

    @classmethod
    def get_projection_taxons(
        cls, projection_taxon_slugs: Iterable[str], taxons: Dict[str, Taxon]
    ) -> List[Tuple[TaxonExpressionStr, Taxon]]:
        return [
            (TaxonExpressionStr(taxon_slug_expression.slug), taxon)
            for taxon_slug_expression, taxon in cls._get_taxons(taxons)
            if taxon_slug_expression.slug in projection_taxon_slugs
        ]

    @classmethod
    def initialize_using_request(
        cls, request: InternalDataRequest, preloaded_taxons: Optional[TaxonMap] = None
    ) -> 'QueryTaxonManager':
        """
        Initializes taxon manager.
        Scope and preagg filters should have corresponding filter_templates created by TelPlanner, and are not
        handled by taxon manager anymore.
        """
        projection_taxon_slugs = set(request.taxons)
        order_by_taxon_slugs = {clause.taxon for clause in (request.order_by or [])}
        filters = request.filters
        filter_taxon_slugs = filters.get_taxon_slugs() if filters else set()

        all_outer_projection_taxon_slugs = projection_taxon_slugs | filter_taxon_slugs | order_by_taxon_slugs

        all_projection_taxons = cls.load_all_taxons(
            request.scope, set(all_outer_projection_taxon_slugs), preloaded_taxons
        )

        return QueryTaxonManager(list(all_outer_projection_taxon_slugs), all_projection_taxons)

    @property
    def all_raw_taxons(self) -> Dict[TaxonSlugExpression, Taxon]:
        """
        This property represents list of all necessary taxons to fetch all relevant data for query

        :return: Map of taxon slug expression to taxon
        """
        return self._raw_taxons

    @property
    def all_selected_raw_taxons(self) -> Dict[TaxonSlugExpression, Taxon]:
        return self._selected_raw_taxons

    @property
    def all_projection_taxons(self) -> List[Tuple[TaxonExpressionStr, Taxon]]:
        """
        This property represents list of taxons to be projected by the final query
        :return: List of taxon slug expressions and taxons meant for projection
        """
        return self._projection_taxons

    @classmethod
    def find_expanded_taxon_subset(
        self, taxon_slugs: Set[str], already_fetched_taxons_arg: TaxonMap
    ) -> Dict[str, Taxon]:
        """
        Method that searches a set of taxon slugs in a dictionary of a previous or "cached" taxon call request
        to avoid loading the same taxons multiple times.

        :param taxon_slugs: Taxon slugs that are to be queried
        :param already_fetched_taxons_arg: A dict of alreasdy known "cache" of loaded taxons
        :return: a subset dictionary of taxon slug to taxons where the slugs from taxon_slugs parameter were found
        """
        subset_dict = {}
        already_fetched_taxons = cast(Dict[str, Taxon], already_fetched_taxons_arg)  #  Type casting for now.
        for taxon_slug in taxon_slugs:
            if taxon_slug in already_fetched_taxons:
                subset_dict[taxon_slug] = already_fetched_taxons[taxon_slug]

                if already_fetched_taxons[taxon_slug].calculation:
                    # in case this is computed taxon, parse its parts - since the computed taxon was already loaded
                    # we already have the computed fetched - otherwise it should throw
                    expression = cast(str, already_fetched_taxons[taxon_slug].calculation)
                    computed_slugs = Tel.get_used_taxon_slugs_shallow(expression).all_slugs

                    for computed_slug in computed_slugs:
                        subset_dict[computed_slug] = already_fetched_taxons[computed_slug]

        return subset_dict

    @classmethod
    def load_all_taxons(
        cls, scope: Scope, taxon_slugs: Set[str], already_fetched_taxons: Optional[TaxonMap] = None
    ) -> Dict[str, Taxon]:
        """
        This method loads  all computed taxons in order to get all their raw tokens, ignoring the taxons in
        the "already fetched slugs set" - i.e. using the cached version rather than querying it live.

        :param scope: Current scope
        :param taxon_slugs: Taxon slugs to expand

        :return: Dictionary with taxon slug as key and its definition as value
        """

        first_taxon_load = True
        loaded_taxons: Dict[str, Taxon] = {}
        # start looking for all taxons you know at the beginning
        missing_taxon_slugs = taxon_slugs
        all_taxon_slugs: Set[str] = set()

        if already_fetched_taxons:
            loaded_taxons = cls.find_expanded_taxon_subset(taxon_slugs, already_fetched_taxons)
            missing_taxon_slugs -= {loaded_taxon_slug for loaded_taxon_slug in loaded_taxons.keys()}

        while missing_taxon_slugs:
            # fetch data for missing taxons
            try:
                taxons_map = Taxonomy.get_taxons_map(
                    company_id=scope.company_id, taxon_slugs=missing_taxon_slugs, throw_if_missing=True
                )
                first_taxon_load = False
            except TaxonsNotFound as error:
                error.severity = ExceptionSeverity.info if first_taxon_load else ExceptionSeverity.error
                raise

            additional_taxons: Set[str] = set()
            # mark that we already fetched these taxons
            all_taxon_slugs |= missing_taxon_slugs

            for taxon_slug, taxon in taxons_map.items():
                try:
                    loaded_taxons[taxon_slug] = taxon
                    if taxon.calculation:  # in case this is computed taxon, parse its parts
                        additional_taxons |= Tel.get_used_taxon_slugs_shallow(taxon.calculation).all_slugs
                except TelExpressionException as error:
                    raise HuskyInvalidTelException(error, taxon.slug)

            # figure out which taxons we are still missing
            missing_taxon_slugs = additional_taxons - all_taxon_slugs

        return loaded_taxons
