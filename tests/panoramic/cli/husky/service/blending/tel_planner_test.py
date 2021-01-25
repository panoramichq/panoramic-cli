from unittest.mock import patch

from panoramic.cli.husky.core.taxonomy.aggregations import AggregationDefinition
from panoramic.cli.husky.core.taxonomy.enums import AggregationType
from panoramic.cli.husky.core.taxonomy.getters import Taxonomy
from panoramic.cli.husky.core.tel.result import PostFormula, PreFormula
from panoramic.cli.husky.core.tel.sql_formula import SqlFormulaTemplate, SqlTemplate
from panoramic.cli.husky.service.blending.blending_taxon_manager import (
    BlendingTaxonManager,
)
from panoramic.cli.husky.service.blending.preprocessing import preprocess_request
from panoramic.cli.husky.service.context import (
    SNOWFLAKE_HUSKY_CONTEXT,
    HuskyQueryContext,
)
from panoramic.cli.husky.service.types.api_data_request_types import BlendingDataRequest
from panoramic.cli.husky.service.types.types import BlendingQueryInfo
from tests.panoramic.cli.husky.test.mocks.core.taxonomy import (
    get_mocked_taxons_by_slug,
    mock_get_taxons,
)
from tests.panoramic.cli.husky.test.test_base import BaseTest


class TestTelPlanner(BaseTest):
    def setUp(self) -> None:
        super().setUp()
        self._twitter_acc_id = 'acc_id_tw123'
        self._fb_acc_id = 'acc_id_456'
        self._blending_request = BlendingDataRequest(
            {
                "data_subrequests": [
                    {
                        "scope": {
                            "preaggregation_filters": {
                                "type": "taxon_value",
                                "taxon": "account_id",
                                "value": self._twitter_acc_id,
                                "operator": "=",
                            }
                        },
                        "properties": {"data_sources": ["twitter"]},
                    },
                    {
                        "scope": {
                            "preaggregation_filters": {
                                "type": "taxon_value",
                                "taxon": "account_id",
                                "value": self._fb_acc_id,
                                "operator": "=",
                            }
                        },
                        "properties": {"data_sources": ["facebook_ads"]},
                    },
                ],
                "taxons": ["fb_tw_merged_objective", "generic_cpm"],
                "limit": 100,
            }
        )
        self._husky_context = HuskyQueryContext.from_request(self._blending_request)
        self._info = BlendingQueryInfo.create(self._blending_request, self._husky_context)

    @patch.object(Taxonomy, '_get_filtered_taxons', side_effect=mock_get_taxons)
    def test_fb_tw_merged_objective_and_generic_cpm(self, mock__get_taxons):
        preprocess_request(self._blending_request)
        taxon_manager = BlendingTaxonManager(self._blending_request)
        taxon_manager.load_all_used_taxons(SNOWFLAKE_HUSKY_CONTEXT)
        plan = taxon_manager.plan
        assert plan.data_source_formula_templates == {
            'facebook_ads': [
                SqlFormulaTemplate(
                    SqlTemplate('''${facebook_ads|objective}'''),
                    '''__fb_tw_merged_objective1''',
                    'facebook_ads',
                    {'facebook_ads|objective'},
                )
            ],
            'twitter': [
                SqlFormulaTemplate(
                    SqlTemplate('''${twitter|objective}'''),
                    '''__fb_tw_merged_objective2''',
                    'twitter',
                    {'twitter|objective'},
                )
            ],
        }
        assert list(map(repr, plan.dimension_formulas)) == [
            repr(
                PreFormula(
                    '''coalesce(__fb_tw_merged_objective1, __fb_tw_merged_objective2)''',
                    '''fb_tw_merged_objective''',
                    AggregationDefinition(type=AggregationType.not_set),
                    None,
                )
            )
        ]
        assert list(map(repr, plan.metric_pre)) == [
            repr(
                PreFormula(
                    '''fb_tw_merged_objective''',
                    '''fb_tw_merged_objective''',
                    AggregationDefinition(type=AggregationType.group_by),
                    None,
                )
            ),
            repr(
                PreFormula(
                    '''1000 * (coalesce(facebook_ads_spend_5811c78c7c741b5a, 0) + coalesce(twitter_spend_68657fbb141b10c8, 0))''',
                    '''__generic_cpm1''',
                    AggregationDefinition(type=AggregationType.sum),
                    None,
                )
            ),
            repr(
                PreFormula(
                    '''coalesce(facebook_ads_impressions_0bf2e36fb4e71190, 0) + coalesce(twitter_impressions_ef12a84724a0ad7d, 0)''',
                    '''__generic_cpm2''',
                    AggregationDefinition(type=AggregationType.sum),
                    None,
                )
            ),
        ]
        expected_merge_taxon = get_mocked_taxons_by_slug(['fb_tw_merged_objective'])[0]
        expected_cpm_taxon = get_mocked_taxons_by_slug(['generic_cpm'])[0]
        assert list(map(repr, plan.metric_post)) == list(
            map(
                repr,
                [
                    (PostFormula('fb_tw_merged_objective', 'fb_tw_merged_objective'), expected_merge_taxon),
                    (
                        PostFormula(
                            '__generic_cpm1 / nullif(__generic_cpm2, 0)', '__generic_cpm1 / nullif(__generic_cpm2, 0)'
                        ),
                        expected_cpm_taxon,
                    ),
                ],
            )
        )
