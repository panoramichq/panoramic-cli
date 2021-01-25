from typing import Dict, List

from panoramic.cli.husky.core.model.enums import ValueQuantityType
from panoramic.cli.husky.service.types.types import DataframeColumn
from panoramic.cli.husky.service.utils.taxon_slug_expression import TaxonExpressionStr
from tests.panoramic.cli.husky.test.mocks.core.taxonomy import mock_get_taxons_map


def get_mocked_dataframe_columns_map(taxon_slugs: List[str]) -> Dict[TaxonExpressionStr, DataframeColumn]:
    """
    Helper fn that creates DataframeColumn map, where all columns are scalar by default.
    :param taxon_slugs:
    :return:
    """
    taxon_map = mock_get_taxons_map(None, taxon_slugs)
    return {
        slug_expr: DataframeColumn(slug_expr, taxon, ValueQuantityType.scalar) for slug_expr, taxon in taxon_map.items()
    }
