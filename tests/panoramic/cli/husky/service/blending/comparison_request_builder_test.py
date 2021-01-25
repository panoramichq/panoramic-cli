from unittest.mock import patch

from panoramic.cli.husky.core.taxonomy.getters import Taxonomy
from panoramic.cli.husky.service.blending.blending_taxon_manager import (
    BlendingTaxonManager,
)
from panoramic.cli.husky.service.blending.comparison_request_builder import (
    ComparisonRequestBuilder,
)
from panoramic.cli.husky.service.context import SNOWFLAKE_HUSKY_CONTEXT
from panoramic.cli.husky.service.types.api_data_request_types import (
    BlendingDataRequest,
    ComparisonConfig,
    InternalDataRequest,
)
from panoramic.cli.husky.service.types.api_scope_types import ComparisonScopeType
from tests.panoramic.cli.husky.test.mocks.core.taxonomy import mock_get_taxons
from tests.panoramic.cli.husky.test.test_base import BaseTest


@patch.object(Taxonomy, '_get_filtered_taxons', side_effect=mock_get_taxons)
class TestComparisonRequestBuilder(BaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.request = BlendingDataRequest(
            {
                "data_subrequests": [
                    {
                        "scope": {
                            "company_id": "50",
                            "preaggregation_filters": {
                                'type': 'group',
                                'logical_operator': 'AND',
                                'clauses': [
                                    {
                                        'type': 'taxon_value',
                                        'taxon': 'account_id',
                                        'operator': '=',
                                        'value': '595126134331606',
                                    },
                                ],
                            },
                        },
                        "properties": {"data_sources": ["facebook_ads"]},
                        "taxons": ["account_id", "spend", "cpm"],
                    }
                ],
                "comparison": ComparisonConfig(
                    {'scope': ComparisonScopeType.company.value, 'taxons': ['objective']}
                ).to_native(),
            }
        )

    def test_company_request(self, mock__get_taxons_map):
        self.request.comparison.scope = ComparisonScopeType.company
        expected = InternalDataRequest(
            {
                'limit': None,
                'offset': None,
                'order_by': [],
                'origin': None,
                'preaggregation_filters': None,
                'properties': {'data_sources': ['facebook_ads'], 'model_name': None},
                'scope': {
                    'company_id': '50',
                    'preaggregation_filters': {
                        'type': 'taxon_value',
                        'taxon': 'company_id',
                        'operator': '=',
                        'value': '50',
                    },
                    'model_visibility': 'available',
                },
                'taxons': ['impressions', 'objective', 'spend'],
            }
        ).to_native()
        self.assertEqual(expected, self._build_comparison_request())

    def _build_comparison_request(self):
        taxon_manager = BlendingTaxonManager(self.request)
        taxon_manager.load_all_used_taxons(SNOWFLAKE_HUSKY_CONTEXT)
        comparison_request = ComparisonRequestBuilder._build_comparison_subrequest(
            self.request.data_subrequests[0], self.request.comparison, taxon_manager
        )
        return comparison_request.to_native()
