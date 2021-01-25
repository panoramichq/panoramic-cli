from typing import Dict, Iterable

from panoramic.cli.husky.core.taxonomy.constants import TEL_EXPR_QUERY_PREFIX
from panoramic.cli.husky.service.types.api_data_request_types import (
    ApiDataRequest,
    BlendingDataRequest,
)
from panoramic.cli.husky.service.utils.exceptions import InvalidRequest
from panoramic.cli.husky.service.utils.taxon_slug_expression import (
    TaxonExpressionStr,
    TaxonSlugExpression,
)


def preprocess_request(req: BlendingDataRequest):
    """
    Helper fn that is moving some values around, to be backward compatible.
    """

    # Move order by from subrequests to top level
    for subrequest in req.data_subrequests:
        if subrequest.order_by:
            req.order_by.extend(subrequest.order_by)
            subrequest.order_by = []

    # Add taxons from grouping sets to top level, so they can be copied to all subrequests.
    # Otherwise computed dimensions would not be included in the result.
    grouping_sets_taxons = {item for sublist in (req.grouping_sets or []) for item in (sublist or [])}
    req.taxons = req.taxons or []
    req.taxons.extend(grouping_sets_taxons)
    move_top_level_to_subrequests(req.taxons, req.data_subrequests)

    if req.grouping_sets and req.fill_date_gaps:
        raise InvalidRequest('request.fill_date_gaps', 'fill_date_gaps is not supported when used with grouping sets.')


def move_top_level_to_subrequests(
    top_level_taxons: Iterable[TaxonExpressionStr], subrequests: Iterable[ApiDataRequest]
):
    # Move top level taxons to subrequests
    ds_to_subrequest: Dict[str, ApiDataRequest] = {
        subrequest.properties.data_sources[0]: subrequest for subrequest in subrequests
    }
    for slug in top_level_taxons or []:
        if slug[0] == TEL_EXPR_QUERY_PREFIX:
            # If taxon is dynamic TEL expr, move it to all subrequests
            for subreq1 in ds_to_subrequest.values():
                subreq1.taxons.append(slug)
        else:
            taxon_expr = TaxonSlugExpression(slug)
            if taxon_expr.data_source is None:
                # If taxon has no data source, move it to all subrequests
                for subreq1 in ds_to_subrequest.values():
                    subreq1.taxons.append(slug)
            else:
                subreq2 = ds_to_subrequest.get(taxon_expr.data_source)
                if subreq2:
                    # Otherwise, move it to subrequest based on data source
                    subreq2.taxons.append(slug)
                else:
                    # Or throw error, if there is no subrequest for given DS
                    raise InvalidRequest(slug, f'Request does not have proper data source for taxon {slug}.')
