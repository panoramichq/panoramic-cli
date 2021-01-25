from typing import Dict, Iterable, List, Mapping, Optional, Set, Union

from panoramic.cli.husky.common.exception_enums import ExceptionSeverity
from panoramic.cli.husky.core.taxonomy.constants import (
    TEL_EXPR_DIMENSION_QUERY_PREFIX,
    TEL_EXPR_METRIC_QUERY_PREFIX,
    TEL_EXPR_QUERY_PREFIX,
)
from panoramic.cli.husky.core.taxonomy.enums import TaxonTypeEnum
from panoramic.cli.husky.core.taxonomy.exceptions import (
    TaxonsNotFound,
    UnexpectedTaxonsFound,
)
from panoramic.cli.husky.core.taxonomy.models import (
    RequestTempTaxon,
    Taxon,
    TaxonTelMetadata,
    taxon_slug_to_sql_friendly_slug,
)
from panoramic.cli.husky.core.tel.evaluator.ast_features import (
    can_become_comparison_metric,
)
from panoramic.cli.husky.core.tel.evaluator.context import TelRootContext, node_id_maker
from panoramic.cli.husky.core.tel.result import (
    UsedTaxonsContainer,
    UsedTaxonSlugsContainer,
)
from panoramic.cli.husky.core.tel.tel import Tel
from panoramic.cli.husky.core.tel.tel_dialect import TaxonTelDialect, TelDialect
from panoramic.cli.husky.core.virtual_state.mappers import VirtualStateMapper
from panoramic.cli.husky.service.context import SNOWFLAKE_HUSKY_CONTEXT
from panoramic.cli.husky.service.utils.taxon_slug_expression import (
    TaxonExpressionStr,
    TaxonMap,
    TaxonSlugExpression,
)
from panoramic.cli.local import get_state


class Taxonomy:
    _all_taxons: Optional[List[Taxon]] = None
    """All loaded taxons"""

    @classmethod
    def preload_taxons(cls, taxons: List[Taxon]):
        """Allows preloading taxons"""
        cls._all_taxons = taxons

    @classmethod
    def preload_taxons_from_state(cls):
        """Preloads taxons from internal state"""
        # get virtual state
        state = get_state()
        # map it to internal state
        internal_state = VirtualStateMapper.to_husky(state)
        cls._all_taxons = internal_state.taxons

    @classmethod
    def precalculate_tel_metadata(cls):
        """Precalculates metadata for all taxons"""
        assert cls._all_taxons is not None, 'Missing taxons'
        for taxon in cls._all_taxons:
            taxon.tel_metadata = get_taxon_tel_metadata(taxon)

    @classmethod
    def _get_filtered_taxons(
        cls,
        company_id: Optional[Iterable[str]] = None,
        taxon_slugs: Optional[Iterable[str]] = None,
        only_computed: Optional[bool] = None,
        data_sources: Optional[Iterable[Optional[str]]] = None,
    ) -> List[Taxon]:
        """
        Loads only specified taxons

        :param company_id:
            - Filters taxons to match the given company_id.
            - Filter is not applied when no company_id is given.
        :param taxon_slugs:
            - Filters taxons to match one of given taxon_slugs.
            - Filter is not applied when no taxon_slugs are given.
        :param only_computed:
            - When "True" returns only taxons with field calculation set to "not null".
        :param data_sources:  List of data sources. None value in list means taxons with no data source.

        :returns: Returns list of selected taxons.
        """
        assert cls._all_taxons is not None, 'Missing taxons. Please preload taxons first'
        all_taxons = cls._all_taxons

        slugs = None if taxon_slugs is None else set(taxon_slugs)
        data_sources = None if data_sources is None else set(data_sources)

        def _check_taxon(taxon):
            return (
                # belongs to company, if requested
                (company_id is None or (company_id is not None and taxon.company_id == company_id))
                and
                # matches a taxon slug, if requested
                (slugs is None or (slugs is not None and taxon.slug in slugs))
                and
                # is computed metric, if only they are requested
                (not only_computed or (only_computed and taxon.is_computed_metric))
                and
                # matches a taxon slug, if requested
                (data_sources is None or (data_sources is not None and taxon.data_source in data_sources))
            )

        selected_taxons = [taxon for taxon in all_taxons if _check_taxon(taxon)]
        return selected_taxons

    @classmethod
    def get_taxons(
        cls,
        company_id: Optional[str] = None,
        taxon_slugs: Optional[Iterable[str]] = None,
        only_computed: Optional[bool] = None,
        data_sources: Optional[Iterable[Optional[str]]] = None,
        throw_if_missing: Optional[bool] = None,
    ) -> List[Taxon]:
        """
        Loads only specified taxons and handle missing taxons, if needed

        :param company_id:
            - Filters taxons to match the given company_id.
            - Filter is not applied when no company_id is given.
        :param taxon_slugs:
            - Filters taxons to match one of given taxon_slugs.
            - Filter is not applied when no taxon_slugs are given.
        :param only_computed:
            - When "True" returns only taxons with field calculation set to "not null".
        :param data_sources:  List of data sources. None value in list means taxons with no data source.
        :param throw_if_missing:    Throw exceptions, if "taxon_slugs" are specified and not all taxons were found.

        :returns: Returns list of selected taxons.
        """
        selected_taxons = cls._get_filtered_taxons(company_id, taxon_slugs, only_computed, data_sources)

        if taxon_slugs is not None and throw_if_missing:
            requested_slugs = set(taxon_slugs)

            # make sure that all requested taxons were found
            received_slugs = set([t.slug for t in selected_taxons])

            if requested_slugs == received_slugs:
                return selected_taxons
            elif requested_slugs > received_slugs:
                raise TaxonsNotFound(taxon_slugs=requested_slugs.difference(received_slugs))
            else:
                raise UnexpectedTaxonsFound(taxon_slugs=received_slugs.difference(requested_slugs))
        else:
            # if we dont care whether all requested taxons were found, just return all you found
            return selected_taxons

    @classmethod
    def get_taxons_map(
        cls, company_id: Optional[str], taxon_slugs: Iterable[str], throw_if_missing: bool = False
    ) -> Dict[str, Taxon]:
        """
        Fetches taxon map by slug

        NOTE:
        Throws when taxons not found or unexpected taxons found.
        """
        selected_taxons = cls.get_taxons(
            company_id=company_id, taxon_slugs=taxon_slugs, throw_if_missing=throw_if_missing
        )

        return {taxon.slug: taxon for taxon in selected_taxons}


class UsedTaxons:
    """Helper class containing getters working with UsedTaxonsContainer"""

    @classmethod
    def get_raw_taxons(
        cls,
        company_id: str,
        taxon_slugs: Iterable[TaxonExpressionStr],
        taxonless_map: TaxonMap,
        data_sources: Optional[Iterable[str]] = None,
        include_aggregation_definition_taxons: bool = False,
    ) -> UsedTaxonsContainer:
        """
        Returns raw (not computed) taxons required to get data for given taxon slugs.
        """
        used_taxons = cls.get_all_taxons(
            company_id,
            taxon_slugs,
            taxonless_map,
            data_sources,
            include_aggregation_definition_taxons,
        )

        raw_taxons = UsedTaxonsContainer()
        raw_taxons.required_taxons = {
            taxon.slug_expr: taxon for taxon in used_taxons.required_taxons.values() if not taxon.is_computed_metric
        }
        raw_taxons.optional_taxons = {
            taxon.slug_expr: taxon for taxon in used_taxons.optional_taxons.values() if not taxon.is_computed_metric
        }
        return raw_taxons

    @classmethod
    def get_all_taxons(
        cls,
        company_id: str,
        taxon_slugs: Iterable[Union[str, TaxonExpressionStr]],
        taxonless_map: TaxonMap,
        data_sources: Optional[Iterable[str]] = None,
        include_aggregation_definition_taxons: bool = False,
    ) -> UsedTaxonsContainer:
        """
        Fetches all taxons requested and all the taxons used, recursively.

        :param company_id: Company ID
        :param taxon_slugs: Load used taxons from these taxons
        :param taxonless_map: Map containing taxons created via taxonless querying
        :param data_sources: Specify relevant data sources
        :param include_aggregation_definition_taxons: Include additional raw taxons from aggregation definitions in the list
        """

        def _load_taxons_directly(slugs, must_load_all_taxons=True):
            proper_slugs = [slug for slug in slugs if not slug.startswith(TEL_EXPR_QUERY_PREFIX)]
            taxonless_slugs = {slug for slug in slugs if slug.startswith(TEL_EXPR_QUERY_PREFIX)}

            # find proper taxons
            if len(proper_slugs):
                taxon_map = Taxonomy.get_taxons_map(
                    company_id=company_id, taxon_slugs=proper_slugs, throw_if_missing=must_load_all_taxons
                )
            else:
                taxon_map = {}

            # find taxons created on the fly
            other_map = {slug: taxon for slug, taxon in taxonless_map.items() if slug in taxonless_slugs}
            return {**taxon_map, **other_map}

        used_taxons = Tel.get_all_used_taxons_map(
            taxon_slugs, data_sources, _load_taxons_directly, include_aggregation_definition_taxons
        )
        return used_taxons

    @classmethod
    def in_taxon_definition(cls, taxon: Taxon) -> UsedTaxonsContainer:
        """
        Retrieve used taxons in a calculation expression and aggregation definition, for the provided company.

        :param taxon: Taxon definition
        :return: Container with used taxons in calculation and aggregation definitions
        """
        final = UsedTaxonsContainer()

        # First get taxons used directly in the calculation
        if taxon.calculation:
            slugs_in_calculation = Tel.get_used_taxon_slugs_shallow(taxon.calculation)
            # Then evaluate that calculation recursively, and get all the taxons used

            # Evaluate optional and required taxon subtrees separately, because all slugs passed to get_used_all_taxons are
            # returned as required, even when they are optional in the calculation we evaluating here.
            required_slugs_subtree = cls.get_all_taxons(taxon.company_id, slugs_in_calculation.required_slugs, {})
            final.update_from(required_slugs_subtree)

            # Anything that came from the optional slugs subtree is optional.
            optional_slugs_subtree = cls.get_all_taxons(taxon.company_id, slugs_in_calculation.optional_slugs, {})
            final.optional_taxons.update(optional_slugs_subtree.required_taxons)
            final.optional_taxons.update(optional_slugs_subtree.optional_taxons)

        if taxon.aggregation:
            # taxon slugs used in aggregation definition of the current taxon are required
            taxon_slugs = taxon.aggregation.used_taxon_slugs()
            found_taxons = Taxonomy.get_taxons(
                company_id=taxon.company_id, taxon_slugs=taxon_slugs, throw_if_missing=True
            )
            taxons: Mapping[TaxonExpressionStr, Taxon] = {
                TaxonExpressionStr(taxon.slug): taxon for taxon in found_taxons
            }
            final.required_taxons.update(taxons)

        return final


def fetch_all_used_taxons_map(
    company_id: str,
    request_formula: Iterable[str],
    data_sources: Optional[Iterable[str]] = None,
    include_aggregation_definition_taxons: bool = True,
) -> TaxonMap:
    """
    Get taxons map based on provided list of request taxon slugs/formulas
    """
    try:
        taxon_slugs_to_get = [slug for slug in request_formula if slug[0] != TEL_EXPR_QUERY_PREFIX]
        formulas_to_get = [slug for slug in request_formula if slug[0] == TEL_EXPR_QUERY_PREFIX]
        taxon_map: TaxonMap = UsedTaxons.get_all_taxons(
            company_id,
            taxon_slugs_to_get,
            {},
            data_sources,
            include_aggregation_definition_taxons=include_aggregation_definition_taxons,
        ).all_taxons

        if formulas_to_get:
            # If there are some formulas starting with =
            taxons_from_formulas_to_get: Set[TaxonExpressionStr] = set()  # Slugs used in those formulas
            formula_slugs = []
            for formula in formulas_to_get:
                # Detect if formula is for metric or dimension
                if formula.startswith(TEL_EXPR_METRIC_QUERY_PREFIX):
                    calculation = formula[len(TEL_EXPR_METRIC_QUERY_PREFIX) :]  # Remove TEL_EXPR_METRIC_QUERY_PREFIX
                    formula_taxon_type = TaxonTypeEnum.metric.value
                elif formula.startswith(TEL_EXPR_DIMENSION_QUERY_PREFIX):
                    calculation = formula[
                        len(TEL_EXPR_DIMENSION_QUERY_PREFIX) :
                    ]  # Remove TEL_EXPR_DIMENSION_QUERY_PREFIX
                    formula_taxon_type = TaxonTypeEnum.dimension.value
                else:
                    raise ValueError(f'Unknown TEL expression {formula}')

                taxons_from_formulas_to_get.update(
                    Tel.get_used_taxon_slugs_shallow(calculation, data_sources).all_slugs
                )
                # Create minimal temporary taxon, so the rest of the query building does not need to change at all.
                # Slug of that taxon is the formula with = in the beginning.

                # Snowflake does not allow " characters as part of SQL identifiers.
                # Ideally, this should be applied only for SQL columns, and not for the final slug.
                # But that would require to many changes at this moment.
                slug = taxon_slug_to_sql_friendly_slug(formula)
                formula_slugs.append(slug)
                formula_taxon = RequestTempTaxon.create_temp_taxon(slug, calculation, formula_taxon_type, company_id)

                taxon_map[TaxonExpressionStr(slug)] = formula_taxon

            taxon_map_from_formulas = UsedTaxons.get_all_taxons(
                company_id, taxons_from_formulas_to_get, {}, data_sources
            ).all_taxons
            taxon_map.update(taxon_map_from_formulas)

            # update TEL metadata for formula taxons
            for slug in formula_slugs:
                taxon = taxon_map[slug]
                taxon.tel_metadata = get_taxon_tel_metadata(taxon)

        return taxon_map
    except TaxonsNotFound as error:
        error.severity = ExceptionSeverity.info
        raise


def get_taxon_tel_metadata(taxon: Taxon) -> TaxonTelMetadata:
    """Calculates TEL metadata for a single taxon"""
    used_taxons = UsedTaxons.in_taxon_definition(taxon)

    # Remove itself.
    taxon_slug = TaxonExpressionStr(taxon.slug)
    if taxon_slug in used_taxons.required_taxons:
        del used_taxons.required_taxons[taxon_slug]
    if taxon_slug in used_taxons.optional_taxons:
        del used_taxons.optional_taxons[taxon_slug]

    required_slugs = {taxon.slug_expr for taxon in used_taxons.required_taxons.values()}
    optional_slugs = {taxon.slug_expr for taxon in used_taxons.optional_taxons.values()}

    raw_required_slugs = {
        taxon.slug_expr for taxon in used_taxons.required_taxons.values() if not taxon.is_computed_metric
    }
    raw_optional_slugs = {
        taxon.slug_expr for taxon in used_taxons.optional_taxons.values() if not taxon.is_computed_metric
    }
    raw_slug_exprs = (TaxonSlugExpression(slug) for slug in (raw_required_slugs | raw_optional_slugs))
    used_data_sources = {slug_expr.data_source for slug_expr in raw_slug_exprs if slug_expr.data_source}

    phase = 0
    can_compute_comparison = False
    if taxon.calculation:
        context = TelRootContext(
            husky_context=SNOWFLAKE_HUSKY_CONTEXT,
            tel_dialect=TaxonTelDialect,
            allowed_data_sources=None,
            taxon_map=used_taxons.all_taxons,
            next_node_id=node_id_maker(),
        )
        tel_expression = TelDialect.visit(taxon.calculation, context, skip_root_node=True)
        phase = tel_expression.phase(context).value
        can_compute_comparison = can_become_comparison_metric(tel_expression)
        aggregation_definition = tel_expression.aggregation_definition(context)
    else:
        # if there is no calculation, use the aggregation definition from taxon definition (raw taxons)
        aggregation_definition = taxon.aggregation

    return TaxonTelMetadata(
        used_data_sources=sorted(list(used_data_sources)),
        required_raw_taxons=sorted(list(raw_required_slugs)),
        optional_raw_taxons=sorted(list(raw_optional_slugs)),
        used_taxons=sorted(list(required_slugs | optional_slugs)),
        phase=phase,
        can_compute_comparison=can_compute_comparison,
        aggregation_definition=aggregation_definition,
    )


def get_used_raw_taxon_slugs_all(
    company_id: str,
    taxon_slugs: Iterable[TaxonExpressionStr],
    data_sources: Iterable[str],
    taxonless_map: TaxonMap,
    include_aggregation_definition_taxons: bool = False,
) -> Iterable[TaxonExpressionStr]:
    """
    Same like get_raw_taxon_slugs, but does not return container with required and optional, but all slugs used.
    Helps with some backward compatible code changes for now.
    """
    return UsedTaxonSlugsContainer.create_from_taxons(
        UsedTaxons.get_raw_taxons(
            company_id,
            taxon_slugs,
            taxonless_map,
            data_sources,
            include_aggregation_definition_taxons,
        )
    ).all_slugs
