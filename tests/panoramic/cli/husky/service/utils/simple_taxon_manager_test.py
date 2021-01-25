from unittest.mock import patch

from panoramic.cli.husky.core.taxonomy.getters import Taxonomy
from panoramic.cli.husky.service.types.api_data_request_types import InternalDataRequest
from panoramic.cli.husky.service.utils.simple_taxon_manager import SimpleTaxonManager
from panoramic.cli.husky.service.utils.taxon_slug_expression import TaxonExpressionStr
from tests.panoramic.cli.husky.test.mocks.core.taxonomy import (
    TAXON_MAP,
    mock_get_taxons,
)
from tests.panoramic.cli.husky.test.mocks.util_sql_template import (
    create_sql_formula_template_raw_taxon,
)
from tests.panoramic.cli.husky.test.test_base import BaseTest


class SimpleTaxonManagerTest(BaseTest):
    @patch.object(Taxonomy, '_get_filtered_taxons', side_effect=mock_get_taxons)
    def test_initialize(self, mock__get_taxons):
        request = InternalDataRequest(
            {
                'scope': {
                    'project_id': 328,
                    'preaggregation_filters': {
                        'type': 'taxon_value',
                        'taxon': 'account_id',
                        'operator': '=',
                        'value': '100',
                    },
                    'company_id': 50,
                },
                'properties': {'data_sources': ['twitter']},
                'taxons': ['impressions', 'campaign_id'],
                'preaggregation_filters': {
                    'type': 'taxon_value',
                    'taxon': 'gender',
                    'operator': '=',
                    'value': 'made up',
                },
            }
        )
        preloaded_taxons = TAXON_MAP
        filter_templates = {
            TaxonExpressionStr('gender'): create_sql_formula_template_raw_taxon('gender', 'twitter'),
            TaxonExpressionStr('account_id'): create_sql_formula_template_raw_taxon('account_id', 'twitter'),
        }
        simple_taxon_manager = SimpleTaxonManager.initialize(request, [], filter_templates, preloaded_taxons)
        expected_graph_select_slugs = {'impressions', 'campaign_id', 'gender', 'account_id'}
        actual_graph_select_slugs = set(taxon.slug for taxon in simple_taxon_manager.graph_select_taxons.values())
        expected_projection_slugs = {'impressions', 'campaign_id'}
        actual_projection_slugs = set(taxon.slug for taxon in simple_taxon_manager.projection_taxons.values())
        self.assertSetEqual(expected_graph_select_slugs, actual_graph_select_slugs)
        self.assertSetEqual(expected_projection_slugs, actual_projection_slugs)
