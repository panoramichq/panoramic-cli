from unittest.mock import patch

from panoramic.cli.husky.core.taxonomy.getters import Taxonomy
from panoramic.cli.husky.service.blending.blending_taxon_manager import (
    BlendingTaxonManager,
)
from panoramic.cli.husky.service.context import SNOWFLAKE_HUSKY_CONTEXT
from panoramic.cli.husky.service.types.api_data_request_types import BlendingDataRequest
from tests.panoramic.cli.husky.test.mocks.core.taxonomy import mock_get_taxons


@patch.object(Taxonomy, '_get_filtered_taxons', side_effect=mock_get_taxons)
def test_get_subrequest_taxons_in_preagg_filter(mock__get_taxons):
    req = BlendingDataRequest(
        {
            'data_subrequests': [
                {
                    'taxons': ['spend'],
                    'preaggregation_filters': {
                        'type': 'taxon_value',
                        'taxon': 'company_id',
                        'operator': '=',
                        'value': '57',
                    },
                    'scope': {
                        'preaggregation_filters': {
                            'type': 'taxon_value',
                            'taxon': 'account_id',
                            'operator': '=',
                            'value': '57',
                        },
                    },
                    'properties': {'data_sources': ['facebook_ads']},
                    'origin': {'system': 'test-case'},
                }
            ],
            'taxons': ['spend'],
            'origin': {'system': 'test-case'},
        }
    )
    manager = BlendingTaxonManager(req)
    manager.load_all_used_taxons(SNOWFLAKE_HUSKY_CONTEXT)
    slugs = manager.get_subrequest_taxons(req.data_subrequests[0])
    assert sorted(slugs) == ['company_id', 'spend']
