import logging
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple, cast

from pydash import flatten

from panoramic.cli.husky.common.exception_enums import ExceptionGroup
from panoramic.cli.husky.common.exception_handler import ExceptionHandler
from panoramic.cli.husky.core.taxonomy.getters import Taxonomy, UsedTaxons
from panoramic.cli.husky.core.taxonomy.models import Taxon
from panoramic.cli.husky.core.tel.exceptions import (
    MissingRequiredTaxonException,
    TelExpressionException,
)
from panoramic.cli.husky.core.tel.result import UsedTaxonSlugsContainer
from panoramic.cli.husky.service.blending.preprocessing import (
    move_top_level_to_subrequests,
)
from panoramic.cli.husky.service.search import HuskySearch
from panoramic.cli.husky.service.types.api_data_request_types import (
    BlendingSearchRequest,
    SearchRequest,
)
from panoramic.cli.husky.service.types.types import (
    TaxonsByDataSource,
    TaxonSlugsByDataSource,
)
from panoramic.cli.husky.service.utils.taxon_slug_expression import (
    TaxonExpressionStr,
    data_source_from_slug,
)

logger = logging.getLogger(__name__)


def _hash_key_get_used_raw_slugs(*args, **kwargs):
    """
    Fn to generate hash key
    """
    slug = args[1].slug
    key = ','.join([slug] + sorted(args[2]))
    return key


class Search:
    @classmethod
    def search_available_taxons(cls, search_request: BlendingSearchRequest) -> TaxonsByDataSource:
        move_top_level_to_subrequests(search_request.taxons, search_request.data_subrequests)
        raw_slugs_by_ds: Dict[str, Set[str]] = dict()
        data_sources = set()
        company_id = search_request.data_subrequests[0].scope.company_id
        all_raw_slugs: Set[str] = set()
        for subrequest in search_request.data_subrequests:
            assert len(subrequest.properties.data_sources) == 1, 'Properties.data_sources must have exactly 1 value.'
            data_source = subrequest.properties.data_sources[0]
            _search_request = SearchRequest(
                dict(
                    taxons=subrequest.taxons,
                    properties=subrequest.properties.to_primitive(),
                    scope=subrequest.scope.to_primitive(),
                )
            )
            logger.debug(f'Starting available raw taxon for ds {data_source}')
            ds_raw_slugs = HuskySearch.get_available_raw_taxons(_search_request)
            logger.debug(f'Completed available raw taxon for ds {data_source}')
            raw_slugs_by_ds[data_source] = ds_raw_slugs
            all_raw_slugs.update(ds_raw_slugs)
            data_sources.add(data_source)

        raw_taxons_by_ds: TaxonsByDataSource = dict()
        for ds, raw_slugs in raw_slugs_by_ds.items():
            logger.debug(f'Starting getting taxons by raw slugs for data source {ds}')
            raw_taxons_by_ds[ds] = Taxonomy.get_taxons(
                taxon_slugs=raw_slugs, company_id=company_id, data_sources=[ds, None]
            )
            logger.debug(f'Completed getting taxons by raw slugs for data source {ds}')
        taxon_allowed_data_sources: List[Optional[str]] = list(data_sources)
        taxon_allowed_data_sources.append(None)  # To get also taxons w/o data source
        logger.debug('Starting getting taxons for all data sources')
        computed_taxons = Taxonomy.get_taxons(
            company_id=company_id, data_sources=taxon_allowed_data_sources, only_computed=True
        )
        logger.debug('Completed getting taxons for all data sources')

        logger.debug('Starting expanding taxons')
        expanded = cls.expand_raw_taxons(raw_taxons_by_ds, computed_taxons)
        logger.debug('Completed expanding taxons')
        return expanded

    @classmethod
    def expand_raw_taxons(
        cls, raw_taxons_by_ds: TaxonsByDataSource, computed_taxons: List[Taxon]
    ) -> TaxonsByDataSource:
        """
        From sets of raw taxons, returns all possible computed taxons.
        """
        taxons_w_wrong_tel = []
        raw_taxons_list = [taxon for taxon in flatten(list(raw_taxons_by_ds.values()))]
        raw_slugs_by_ds = {ds: {t.slug for t in taxons} for ds, taxons in raw_taxons_by_ds.items()}
        raw_taxon_slugs = {taxon.slug for taxon in raw_taxons_list}
        requested_data_sources = set(raw_taxons_by_ds.keys())
        result: Dict[str, List[Taxon]] = {ds: taxons for ds, taxons in raw_taxons_by_ds.items()}
        for computed_taxon in computed_taxons:
            try:
                required_slugs, used_slugs_by_ds = cls._get_used_raw_taxon_slugs(computed_taxon)
                if not required_slugs.issubset(raw_taxon_slugs):
                    # Some required taxon slug is missing, slug is not compatible.
                    continue

                # Used taxons that dont have ds. For backward compatibility as long as we have taxons w/o ds.
                none_ds_slugs = used_slugs_by_ds.get(None, set())

                # True if taxon is e.g. a constant, such as t=1+1
                taxon_has_no_used_taxons = len(used_slugs_by_ds) == 0

                for ds in requested_data_sources:
                    # For every DS, check if the taxon is compatible..
                    ds_used_slugs = used_slugs_by_ds.get(ds, set())
                    ds_raw_slugs = raw_slugs_by_ds[ds]
                    has_needed_taxons = False
                    if ds_used_slugs or none_ds_slugs:
                        if ds_used_slugs.issubset(ds_raw_slugs) and none_ds_slugs.issubset(ds_raw_slugs):
                            # All DS or None-DS needed raw taxons are available
                            has_needed_taxons = True
                    if has_needed_taxons or taxon_has_no_used_taxons:
                        result[ds].append(computed_taxon)

            except MissingRequiredTaxonException:
                pass
            except Exception:
                # Whenever exception occurs, we just note the taxon slug and continue to not break the whole request.
                taxons_w_wrong_tel.append(computed_taxon.slug)

        if taxons_w_wrong_tel:
            # Log all Tel exceptions at once
            # Not nicest, but gets the job done for now.
            taxons_w_wrong_tel_str = ','.join(taxons_w_wrong_tel)
            tel_exception = TelExpressionException(f'Wrong tel exception in taxons: {taxons_w_wrong_tel_str}')
            ExceptionHandler.track_exception(tel_exception, ExceptionGroup.TAXONS)

        return result

    @classmethod
    def _taxons_to_slugs_by_ds(cls, used_taxons: UsedTaxonSlugsContainer) -> TaxonSlugsByDataSource:
        """
        Converts all slugs into by DS dict.
        """
        result: TaxonSlugsByDataSource = defaultdict(set)
        for slug in used_taxons.all_slugs:
            result[data_source_from_slug(slug)].add(slug)
        return result

    @classmethod
    def _used_slugs_from_tel_metadata(cls, taxon: Taxon) -> UsedTaxonSlugsContainer:
        """
        Returns tel_metadata as used slugs container.
        """
        used_slugs = UsedTaxonSlugsContainer()
        assert taxon.tel_metadata is not None  # Just so mypy shuts up
        used_slugs.required_slugs = cast(Set[TaxonExpressionStr], set(taxon.tel_metadata.required_raw_taxons or []))
        used_slugs.optional_slugs = cast(Set[TaxonExpressionStr], set(taxon.tel_metadata.optional_raw_taxons or []))

        return used_slugs

    @classmethod
    def _get_used_raw_taxon_slugs(cls, taxon: Taxon) -> Tuple[Set[TaxonExpressionStr], TaxonSlugsByDataSource]:
        """
        Cached fn that stored all used raw slugs. Using taxon slug and data sources as key.
        Returns required slugs, and then slugs split by data source.
        """
        if taxon.tel_metadata:
            used_taxons = cls._used_slugs_from_tel_metadata(taxon)
        else:
            #  If metadata is not calculated, we can still compute it, but might take a bit of time to load all needed
            # taxons from DB, as well as traverse the whole TEL tree
            used_taxons = UsedTaxonSlugsContainer.create_from_taxons(
                UsedTaxons.get_raw_taxons(taxon.company_id, [taxon.slug_expr], {})
            )
        required_slugs = used_taxons.required_slugs
        slugs_by_ds = cls._taxons_to_slugs_by_ds(used_taxons)
        return required_slugs, slugs_by_ds
