from typing import List, Optional
from unittest.mock import patch

import pytest

from panoramic.cli.datacol.tztools import now
from panoramic.cli.husky.core.sql_alchemy_util import compile_query
from panoramic.cli.husky.core.taxonomy.getters import Taxonomy
from panoramic.cli.husky.core.taxonomy.override_mapping.enums import MappingSourceType
from panoramic.cli.husky.core.taxonomy.override_mapping.models import OverrideMapping
from panoramic.cli.husky.core.tel.sql_formula import SqlFormulaTemplate, SqlTemplate
from panoramic.cli.husky.service.blending.query_builder import QueryBuilder
from panoramic.cli.husky.service.context import HuskyQueryContext
from panoramic.cli.husky.service.filter_builder.filter_clauses import (
    TaxonValueFilterClause,
)
from panoramic.cli.husky.service.types.api_data_request_types import (
    BlendingDataRequest,
    ComparisonConfig,
    InternalDataRequest,
    TaxonDataOrder,
)
from panoramic.cli.husky.service.types.enums import HuskyQueryRuntime
from panoramic.cli.husky.service.types.types import BlendingQueryInfo, Dataframe
from panoramic.cli.husky.service.utils.exceptions import (
    HuskyInvalidTelException,
    ModelNotFoundException,
    TooManyPhysicalDataSourcesException,
)
from tests.panoramic.cli.husky.test.mocks.core.taxonomy import mock_get_taxons
from tests.panoramic.cli.husky.test.mocks.husky.mock_single_query import (
    create_single_query_mock,
)
from tests.panoramic.cli.husky.test.mocks.husky_model import (
    MOCK_DATA_SOURCE_NAME,
    get_mock_metric_model,
    get_mock_physical_data_sources_model,
)
from tests.panoramic.cli.husky.test.mocks.mock_dataframe import (
    get_mocked_dataframe_columns_map,
)
from tests.panoramic.cli.husky.test.mocks.util_sql_template import (
    create_sql_formula_template_raw_taxon,
)
from tests.panoramic.cli.husky.test.test_base import BaseTest
from tests.panoramic.cli.husky.test.util import TEST_COMPANY_ID_50


class TestDataframeBlending(BaseTest):
    def setUp(self) -> None:
        super().setUp()
        self._adwords_acc_id = 'acc_id_123'
        self._fb_acc_id = 'acc_id_456'
        self._blending_request = BlendingDataRequest(
            {
                "data_subrequests": [
                    {
                        "scope": {
                            "company_id": TEST_COMPANY_ID_50,
                            "preaggregation_filters": {
                                "type": "taxon_value",
                                "taxon": "account_id",
                                "value": self._adwords_acc_id,
                                "operator": "=",
                            },
                        },
                        "properties": {"data_sources": ["adwords"]},
                        "taxons": ["date", "spend", "cpm"],
                    },
                    {
                        "scope": {
                            "company_id": TEST_COMPANY_ID_50,
                            "preaggregation_filters": {
                                "type": "taxon_value",
                                "taxon": "account_id",
                                "value": self._fb_acc_id,
                                "operator": "=",
                            },
                        },
                        "properties": {"data_sources": ["facebook_ads"]},
                        "taxons": ["date", "spend", "cpm"],
                    },
                ],
                "order_by": [{"taxon": "date", "type": "asc"}],
                "limit": 100,
            }
        )
        self._husky_context = HuskyQueryContext.from_request(self._blending_request)
        self._info = BlendingQueryInfo.create(self._blending_request, self._husky_context)

    @patch.object(Taxonomy, '_get_filtered_taxons', side_effect=mock_get_taxons)
    @patch('panoramic.cli.husky.service.query_builder.QueryBuilder.build_query')
    def test_full_blending_no_comparison(self, mock__husky_build_query, mock__get_taxons):
        self._blending_request.comparison = None
        self._blending_request.data_subrequests[0].order_by = [TaxonDataOrder({"taxon": "date", "type": "asc"})]
        adwords_df = Dataframe(
            create_single_query_mock('adwords'),
            get_mocked_dataframe_columns_map(['spend', 'impressions', 'date']),
            {'adwords_table'},
            {'context'},
        )
        fb_df = Dataframe(
            create_single_query_mock('facebook'),
            get_mocked_dataframe_columns_map(['spend', 'impressions', 'date']),
            {'fb_table'},
            {'context'},
        )

        mock__husky_build_query.side_effect = [adwords_df, fb_df]

        df = QueryBuilder.build_query(self._husky_context, self._blending_request, self._info)
        self.write_test_expectations('query.sql', compile_query(df.query))
        expected_query = self.read_test_expectations('query.sql')
        self.assertEqual(expected_query, compile_query(df.query))
        self.assertEqual(['cpm', 'date', 'spend'], sorted(df.slug_to_column.keys()))
        self.assertSetEqual({'adwords_table', 'fb_table'}, df.used_model_names)

    @patch.object(Taxonomy, '_get_filtered_taxons', side_effect=mock_get_taxons)
    @patch('panoramic.cli.husky.service.query_builder.QueryBuilder.build_query')
    def test_no_comparison_only_postagg_filters(self, mock__husky_build_query, mock__get_taxons):
        self._blending_request.comparison = None
        self._blending_request.filters = TaxonValueFilterClause(
            {"type": "taxon_value", "taxon": "facebook_ads|impressions", "value": 0, "operator": ">"}
        )

        adwords_df = Dataframe(
            create_single_query_mock('adwords'),
            get_mocked_dataframe_columns_map(['spend', 'impressions', 'date']),
            {'adwords_table'},
            {'context'},
        )
        fb_df = Dataframe(
            create_single_query_mock('facebook'),
            get_mocked_dataframe_columns_map(['spend', 'impressions', 'date']),
            {'fb_table'},
            {'context'},
        )

        mock__husky_build_query.side_effect = [adwords_df, fb_df]

        df = QueryBuilder.build_query(self._husky_context, self._blending_request, self._info)
        self.write_test_expectations('query.sql', compile_query(df.query))
        expected_query = self.read_test_expectations('query.sql')
        self.assertEqual(expected_query, compile_query(df.query))
        self.assertEqual(['date', 'spend', 'cpm'], list(df.slug_to_column.keys()))
        self.assertSetEqual({'adwords_table', 'fb_table'}, df.used_model_names)

    @patch.object(Taxonomy, '_get_filtered_taxons', side_effect=mock_get_taxons)
    @patch('panoramic.cli.husky.service.query_builder.QueryBuilder.build_query')
    def test_full_blending_simple_comparison(self, mock__husky_build_query, mock__get_taxons):
        self._blending_request.comparison = ComparisonConfig({"taxons": ["objective"]})
        adwords_df = Dataframe(
            create_single_query_mock('adwords'),
            get_mocked_dataframe_columns_map(['spend', 'impressions', 'objective', 'date']),
            {'adwords_table'},
            {'context'},
        )
        fb_df = Dataframe(
            create_single_query_mock('facebook'),
            get_mocked_dataframe_columns_map(['spend', 'impressions', 'objective', 'date']),
            {'fb_table'},
            {'context'},
        )

        adwords_comparison_df = Dataframe(
            create_single_query_mock('adwords_comparison'),
            get_mocked_dataframe_columns_map(['spend', 'impressions', 'objective']),
            {'adwords_comparison_table'},
            {'context'},
        )

        fb_comparison_df = Dataframe(
            create_single_query_mock('facebook_comparison'),
            get_mocked_dataframe_columns_map(['spend', 'impressions', 'objective']),
            {'fb_comparison_table'},
            {'context'},
        )

        mock__husky_build_query.side_effect = [adwords_df, fb_df, adwords_comparison_df, fb_comparison_df]

        df = QueryBuilder.build_query(self._husky_context, self._blending_request, self._info)
        self.write_test_expectations('query.sql', compile_query(df.query))
        expected_query = self.read_test_expectations('query.sql')
        self.assertEqual(expected_query, compile_query(df.query))
        self.assertEqual(['comparison@cpm', 'date', 'spend', 'cpm'], list(df.slug_to_column.keys()))

        # Check that adwords comparison data request was correctly created and passed to builder query.
        comparsion_adwords_husky_request: InternalDataRequest = mock__husky_build_query.call_args_list[2][0][1]
        # Added objective taxon
        self.assertEqual(['impressions', 'objective', 'spend'], comparsion_adwords_husky_request.taxons)
        self.assertEqual(
            {'fb_comparison_table', 'adwords_comparison_table', 'adwords_table', 'fb_table'}, df.used_model_names
        )

    @patch.object(Taxonomy, '_get_filtered_taxons', side_effect=mock_get_taxons)
    @patch('panoramic.cli.husky.service.query_builder.QueryBuilder.build_query')
    def test_full_blending_advanced_comparison(self, mock__husky_build_query, mock__get_taxons):
        self._blending_request.comparison = ComparisonConfig({"taxons": ["objective", "date", "ad_id"]})
        adwords_df = Dataframe(
            create_single_query_mock('adwords'),
            get_mocked_dataframe_columns_map(['spend', 'impressions', 'objective', 'date', 'ad_id']),
            {'adwords_table'},
            {'context'},
        )
        fb_df = Dataframe(
            create_single_query_mock('facebook'),
            get_mocked_dataframe_columns_map(['spend', 'impressions', 'objective', 'date', 'ad_id']),
            {'fb_table'},
            {'context'},
        )

        adwords_comparison_df = Dataframe(
            create_single_query_mock('adwords_comparison'),
            get_mocked_dataframe_columns_map(['spend', 'impressions', 'objective', 'ad_id', 'date']),
            {'adwords_comparison_table'},
            {'context'},
        )

        fb_comparison_df = Dataframe(
            create_single_query_mock('facebook_comparison'),
            get_mocked_dataframe_columns_map(['spend', 'impressions', 'objective', 'ad_id', 'date']),
            {'fb_comparison_table'},
            {'context'},
        )

        mock__husky_build_query.side_effect = [adwords_df, fb_df, adwords_comparison_df, fb_comparison_df]

        df = QueryBuilder.build_query(self._husky_context, self._blending_request, self._info)
        self.write_test_expectations('query.sql', compile_query(df.query))
        expected_query = self.read_test_expectations('query.sql')
        self.assertEqual(expected_query, compile_query(df.query))
        self.assertEqual(['comparison@cpm', 'date', 'spend', 'cpm'], list(df.slug_to_column.keys()))

        # Check that adwords comparison data request was correctly created and passed to builder query.
        comparsion_adwords_husky_request: InternalDataRequest = mock__husky_build_query.call_args_list[2][0][1]
        # Added objective, date, ad_id taxons
        self.assertSetEqual(
            {'ad_id', 'date', 'impressions', 'objective', 'spend'}, set(comparsion_adwords_husky_request.taxons)
        )
        self.assertSetEqual(
            {'fb_comparison_table', 'adwords_comparison_table', 'adwords_table', 'fb_table'}, df.used_model_names
        )

    @patch.object(Taxonomy, '_get_filtered_taxons', side_effect=mock_get_taxons)
    @patch('panoramic.cli.husky.service.query_builder.QueryBuilder.build_query')
    def test_full_blending_with_grouping_sets(self, mock__husky_build_query, mock__get_taxons):
        self._blending_request.comparison = ComparisonConfig({"taxons": ["objective"]})
        self._blending_request.order_by = []
        self._blending_request.grouping_sets = [
            ['campaign_id'],
            ['campaign_id', 'adgroup_id'],
            ['campaign_id', 'adgroup_id', 'ad_id'],
            [],
        ]
        adwords_df = Dataframe(
            create_single_query_mock('adwords'),
            get_mocked_dataframe_columns_map(
                ['spend', 'impressions', 'date', 'campaign_id', 'adgroup_id', 'ad_id', 'objective']
            ),
            {'adwords_table'},
            {'context'},
        )
        fb_df = Dataframe(
            create_single_query_mock('facebook'),
            get_mocked_dataframe_columns_map(
                ['spend', 'impressions', 'date', 'campaign_id', 'adgroup_id', 'ad_id', 'objective']
            ),
            {'fb_table'},
            {'context'},
        )

        adwords_comparison_df = Dataframe(
            create_single_query_mock('adwords_comparison'),
            get_mocked_dataframe_columns_map(['spend', 'impressions', 'objective']),
            {'adwords_comparison_table'},
            {'context'},
        )

        fb_comparison_df = Dataframe(
            create_single_query_mock('facebook_comparison'),
            get_mocked_dataframe_columns_map(['spend', 'impressions', 'objective']),
            {'fb_comparison_table'},
            {'context'},
        )

        mock__husky_build_query.side_effect = [adwords_df, fb_df, adwords_comparison_df, fb_comparison_df]

        df = QueryBuilder.build_query(self._husky_context, self._blending_request, self._info)
        self.write_test_expectations('query.sql', compile_query(df.query))
        expected_query = self.read_test_expectations('query.sql')
        self.assertEqual(expected_query, compile_query(df.query))
        self.assertSetEqual(
            {'adgroup_id', 'campaign_id', 'ad_id', 'spend', 'cpm', 'comparison@cpm'}, set(df.slug_to_column.keys())
        )
        self.assertSetEqual(
            {'adwords_table', 'fb_table', 'adwords_comparison_table', 'fb_comparison_table'}, df.used_model_names
        )


class TestDataframeBlendingEnhancedCpm(BaseTest):
    def setUp(self) -> None:
        super().setUp()
        self._adwords_acc_id = 'acc_id_123'
        self._fb_acc_id = 'acc_id_456'
        self._blending_request = BlendingDataRequest(
            {
                "data_subrequests": [
                    {
                        "scope": {
                            "company_id": TEST_COMPANY_ID_50,
                            "preaggregation_filters": {
                                "type": "taxon_value",
                                "taxon": "account_id",
                                "value": self._adwords_acc_id,
                                "operator": "=",
                            },
                        },
                        "properties": {"data_sources": ["adwords"]},
                        "taxons": ["enhanced_cpm"],
                    },
                    {
                        "scope": {
                            "company_id": TEST_COMPANY_ID_50,
                            "preaggregation_filters": {
                                "type": "taxon_value",
                                "taxon": "account_id",
                                "value": self._fb_acc_id,
                                "operator": "=",
                            },
                        },
                        "properties": {"data_sources": ["facebook_ads"]},
                        "taxons": ["enhanced_cpm"],
                    },
                ],
                "limit": 100,
            }
        )
        self._husky_context = HuskyQueryContext.from_request(self._blending_request)
        self._info = BlendingQueryInfo.create(self._blending_request, self._husky_context)

    @patch.object(Taxonomy, '_get_filtered_taxons', side_effect=mock_get_taxons)
    @patch('panoramic.cli.husky.service.query_builder.QueryBuilder.build_query')
    def test_full_blending_comparison(self, mock__husky_build_query, mock__get_taxons):
        self._blending_request.comparison = ComparisonConfig({"taxons": ["objective"]})
        adwords_df = Dataframe(
            create_single_query_mock('adwords'),
            get_mocked_dataframe_columns_map(['generic_spend', 'objective', 'generic_impressions']),
            {'adwords_table'},
            {'context'},
        )
        fb_df = Dataframe(
            create_single_query_mock('facebook'),
            get_mocked_dataframe_columns_map(['generic_spend', 'objective', 'generic_impressions']),
            {'fb_table'},
            {'context'},
        )

        adwords_comparison_df = Dataframe(
            create_single_query_mock('adwords_comparison'),
            get_mocked_dataframe_columns_map(['generic_spend', 'objective', 'generic_impressions']),
            {'adwords_comparison_table'},
            {'context'},
        )
        fb_comparison_df = Dataframe(
            create_single_query_mock('facebook_comparison'),
            get_mocked_dataframe_columns_map(['generic_spend', 'objective', 'generic_impressions']),
            {'fb_comparison_table'},
            {'context'},
        )

        mock__husky_build_query.side_effect = [adwords_df, fb_df, adwords_comparison_df, fb_comparison_df]

        df = QueryBuilder.build_query(self._husky_context, self._blending_request, self._info)
        self.write_test_expectations('query.sql', compile_query(df.query))
        expected_query = self.read_test_expectations('query.sql')
        self.assertEqual(expected_query, compile_query(df.query))
        self.assertSetEqual({'enhanced_cpm', 'comparison@enhanced_cpm'}, set(df.slug_to_column.keys()))
        self.assertSetEqual(
            {'adwords_table', 'fb_table', 'adwords_comparison_table', 'fb_comparison_table'}, df.used_model_names
        )


class TestDataframeBlendingCumulativeSum(BaseTest):
    def setUp(self) -> None:
        super().setUp()
        self._adwords_acc_id = 'acc_id_123'
        self._fb_acc_id = 'acc_id_456'
        self._blending_request = BlendingDataRequest(
            {
                "data_subrequests": [
                    {
                        "scope": {
                            "company_id": TEST_COMPANY_ID_50,
                            "preaggregation_filters": {
                                "type": "taxon_value",
                                "taxon": "account_id",
                                "value": self._adwords_acc_id,
                                "operator": "=",
                            },
                        },
                        "properties": {"data_sources": ["adwords"]},
                        "taxons": [
                            "generic_spend",
                            "enhanced_spend",
                            "gender",
                            "date",
                        ],
                    }
                ],
                "limit": 100,
            }
        )
        self._husky_context = HuskyQueryContext.from_request(self._blending_request)
        self._info = BlendingQueryInfo.create(self._blending_request, self._husky_context)

    @patch.object(Taxonomy, '_get_filtered_taxons', side_effect=mock_get_taxons)
    @patch('panoramic.cli.husky.service.query_builder.QueryBuilder.build_query')
    def test_full_blending_comparison(self, mock__husky_build_query, mock__get_taxons):
        adwords_df = Dataframe(
            create_single_query_mock('adwords'),
            get_mocked_dataframe_columns_map(['generic_spend', 'objective', 'generic_impressions', 'gender']),
            {'adwords_table'},
            {'context'},
        )

        mock__husky_build_query.side_effect = [adwords_df]

        df = QueryBuilder.build_query(self._husky_context, self._blending_request, self._info)
        self.write_test_expectations('query.sql', compile_query(df.query))
        expected_query = self.read_test_expectations('query.sql')
        assert compile_query(df.query) == expected_query
        self.assertSetEqual(
            set(df.slug_to_column.keys()),
            {
                "generic_spend",
                "enhanced_spend",
                "gender",
                "date",
            },
        )
        self.assertSetEqual(df.used_model_names, {'adwords_table'})


class TestTaxonlessQuerying(BaseTest):
    def setUp(self) -> None:
        super().setUp()
        self._adwords_acc_id = 'acc_id_123'
        self._fb_acc_id = 'acc_id_456'
        self._blending_request = BlendingDataRequest(
            {
                "data_subrequests": [
                    {
                        "scope": {
                            "company_id": TEST_COMPANY_ID_50,
                            "preaggregation_filters": {
                                "type": "taxon_value",
                                "taxon": "account_id",
                                "value": self._adwords_acc_id,
                                "operator": "=",
                            },
                        },
                        "properties": {"data_sources": ["adwords"]},
                        "taxons": ["enhanced_cpm"],
                    },
                    {
                        "scope": {
                            "company_id": TEST_COMPANY_ID_50,
                            "preaggregation_filters": {
                                "type": "taxon_value",
                                "taxon": "account_id",
                                "value": self._fb_acc_id,
                                "operator": "=",
                            },
                        },
                        "properties": {"data_sources": ["facebook_ads"]},
                        "taxons": ["enhanced_cpm"],
                    },
                ],
                "limit": 100,
                "taxons": ["spend", "=m:generic_spend / generic_impressions"],
            }
        )
        self._husky_context = HuskyQueryContext.from_request(self._blending_request)
        self._info = BlendingQueryInfo.create(self._blending_request, self._husky_context)

    @patch.object(Taxonomy, '_get_filtered_taxons', side_effect=mock_get_taxons)
    @patch('panoramic.cli.husky.service.query_builder.QueryBuilder.build_query')
    def test_one(self, mock__husky_build_query, mock__get_taxons):
        adwords_df = Dataframe(
            create_single_query_mock('adwords'),
            get_mocked_dataframe_columns_map(['generic_spend', 'generic_impressions']),
            {'adwords_table'},
            {'context'},
        )
        fb_df = Dataframe(
            create_single_query_mock('facebook'),
            get_mocked_dataframe_columns_map(['generic_spend', 'generic_impressions']),
            {'fb_table'},
            {'context'},
        )

        mock__husky_build_query.side_effect = [adwords_df, fb_df]

        df = QueryBuilder.build_query(self._husky_context, self._blending_request, self._info)
        self.write_test_expectations('query.sql', compile_query(df.query))
        expected_query = self.read_test_expectations('query.sql')
        self.assertEqual(expected_query, compile_query(df.query))
        self.assertSetEqual(
            {'enhanced_cpm', 'spend', '=m:generic_spend / generic_impressions'}, set(df.slug_to_column.keys())
        )
        self.assertSetEqual({'adwords_table', 'fb_table'}, df.used_model_names)


class TestTelInSubrequestsQuerying(BaseTest):
    def setUp(self) -> None:
        super().setUp()
        self._adwords_acc_id = 'acc_id_123'
        self._twitter_acc_id = 'acc_id_tw123'
        self._fb_acc_id = 'acc_id_456'

    @patch.object(Taxonomy, '_get_filtered_taxons', side_effect=mock_get_taxons)
    @patch('panoramic.cli.husky.service.query_builder.QueryBuilder.build_query')
    def test_taxon_in_some_subrequests(self, mock__husky_build_query, mock__get_taxons):
        self._blending_request = BlendingDataRequest(
            {
                "data_subrequests": [
                    {
                        "scope": {
                            "company_id": TEST_COMPANY_ID_50,
                            "preaggregation_filters": {
                                "type": "taxon_value",
                                "taxon": "account_id",
                                "value": self._adwords_acc_id,
                                "operator": "=",
                            },
                        },
                        "properties": {"data_sources": ["adwords"]},
                        "taxons": ["fb_tw_adwords_impressions_all_optional"],
                    },
                    {
                        "scope": {
                            "company_id": TEST_COMPANY_ID_50,
                            "preaggregation_filters": {
                                "type": "taxon_value",
                                "taxon": "account_id",
                                "value": self._fb_acc_id,
                                "operator": "=",
                            },
                        },
                        "properties": {"data_sources": ["facebook_ads"]},
                        "taxons": ["fb_tw_adwords_spend_all_optional"],
                    },
                ],
                "limit": 100,
            }
        )
        self._husky_context = HuskyQueryContext.from_request(self._blending_request)
        self._info = BlendingQueryInfo.create(self._blending_request, self._husky_context)
        adwords_df = Dataframe(
            create_single_query_mock('adwords'),
            get_mocked_dataframe_columns_map(['adwords|impressions']),
            {'adwords_table'},
            {'context'},
        )
        fb_df = Dataframe(
            create_single_query_mock('facebook'),
            get_mocked_dataframe_columns_map(['facebook_ads|spend']),
            {'fb_table'},
            {'context'},
        )

        mock__husky_build_query.side_effect = [adwords_df, fb_df]

        df = QueryBuilder.build_query(self._husky_context, self._blending_request, self._info)
        self.write_test_expectations('query.sql', compile_query(df.query))
        expected_query = self.read_test_expectations('query.sql')
        self.assertEqual(expected_query, compile_query(df.query))
        #  Check that correct raw taxons are requested from Husky.build_query
        self.assertEqual({'adwords|impressions'}, set(mock__husky_build_query.call_args_list[0][0][1].taxons))
        self.assertEqual({'facebook_ads|spend'}, set(mock__husky_build_query.call_args_list[1][0][1].taxons))

    @patch.object(Taxonomy, '_get_filtered_taxons', side_effect=mock_get_taxons)
    @patch('panoramic.cli.husky.service.query_builder.QueryBuilder.build_query')
    def test_taxon_in_all_subrequests(self, mock__husky_build_query, mock_get_taxons):
        self._blending_request = BlendingDataRequest(
            {
                "data_subrequests": [
                    {
                        "scope": {
                            "company_id": TEST_COMPANY_ID_50,
                            "preaggregation_filters": {
                                "type": "taxon_value",
                                "taxon": "account_id",
                                "value": self._adwords_acc_id,
                                "operator": "=",
                            },
                        },
                        "properties": {"data_sources": ["adwords"]},
                        "taxons": ["fb_tw_adwords_impressions_all_optional", "fb_tw_adwords_spend_all_optional"],
                    },
                    {
                        "scope": {
                            "company_id": TEST_COMPANY_ID_50,
                            "preaggregation_filters": {
                                "type": "taxon_value",
                                "taxon": "account_id",
                                "value": self._fb_acc_id,
                                "operator": "=",
                            },
                        },
                        "properties": {"data_sources": ["facebook_ads"]},
                        "taxons": ["fb_tw_adwords_impressions_all_optional", "fb_tw_adwords_spend_all_optional"],
                    },
                ],
                "limit": 100,
            }
        )
        self._husky_context = HuskyQueryContext.from_request(self._blending_request)
        self._info = BlendingQueryInfo.create(self._blending_request, self._husky_context)
        adwords_df = Dataframe(
            create_single_query_mock('adwords'),
            get_mocked_dataframe_columns_map(['adwords|impressions']),
            {'adwords_table'},
            {'context'},
        )
        fb_df = Dataframe(
            create_single_query_mock('facebook'),
            get_mocked_dataframe_columns_map(['facebook_ads|spend']),
            {'fb_table'},
            {'context'},
        )

        mock__husky_build_query.side_effect = [adwords_df, fb_df]

        df = QueryBuilder.build_query(self._husky_context, self._blending_request, self._info)
        self.write_test_expectations('query.sql', compile_query(df.query))
        expected_query = self.read_test_expectations('query.sql')
        self.assertEqual(expected_query, compile_query(df.query))
        #  Check that account filter template is correctly passed to single husky
        self.assertEqual(
            {'account_id': create_sql_formula_template_raw_taxon('account_id', 'adwords')},
            mock__husky_build_query.call_args_list[0][1]['filter_templates'],
        )
        self.assertEqual(
            {'account_id': create_sql_formula_template_raw_taxon('account_id', 'facebook_ads')},
            mock__husky_build_query.call_args_list[1][1]['filter_templates'],
        )

    @patch.object(Taxonomy, '_get_filtered_taxons', side_effect=mock_get_taxons)
    @patch('panoramic.cli.husky.service.query_builder.QueryBuilder.build_query')
    def test_required_taxon_in_all_subrequests(self, mock__husky_build_query, mock__get_taxons):
        self._blending_request = BlendingDataRequest(
            {
                "data_subrequests": [
                    {
                        "scope": {
                            "company_id": TEST_COMPANY_ID_50,
                            "preaggregation_filters": {
                                "type": "taxon_value",
                                "taxon": "account_id",
                                "value": self._adwords_acc_id,
                                "operator": "=",
                            },
                        },
                        "properties": {"data_sources": ["adwords"]},
                        "taxons": ["fb_adwords_spend_all_required"],
                    },
                    {
                        "scope": {
                            "company_id": TEST_COMPANY_ID_50,
                            "preaggregation_filters": {
                                "type": "taxon_value",
                                "taxon": "account_id",
                                "value": self._fb_acc_id,
                                "operator": "=",
                            },
                        },
                        "properties": {"data_sources": ["facebook_ads"]},
                        "taxons": ["fb_adwords_spend_all_required"],
                    },
                ],
                "limit": 100,
            }
        )
        self._husky_context = HuskyQueryContext.from_request(self._blending_request)
        self._info = BlendingQueryInfo.create(self._blending_request, self._husky_context)
        adwords_df = Dataframe(
            create_single_query_mock('adwords'),
            get_mocked_dataframe_columns_map(['adwords|spend']),
            {'adwords_table'},
            {'context'},
        )
        fb_df = Dataframe(
            create_single_query_mock('facebook'),
            get_mocked_dataframe_columns_map(['facebook_ads|spend']),
            {'fb_table'},
            {'context'},
        )

        mock__husky_build_query.side_effect = [adwords_df, fb_df]

        df = QueryBuilder.build_query(self._husky_context, self._blending_request, self._info)
        self.write_test_expectations('query.sql', compile_query(df.query))
        expected_query = self.read_test_expectations('query.sql')
        self.assertEqual(expected_query, compile_query(df.query))

    @patch.object(Taxonomy, '_get_filtered_taxons', side_effect=mock_get_taxons)
    @patch('panoramic.cli.husky.service.query_builder.QueryBuilder.build_query')
    def test_required_taxon_missing_ds_completely(self, mock__husky_build_query, mock__get_taxons):
        self._blending_request = BlendingDataRequest(
            {
                "data_subrequests": [
                    {
                        "scope": {
                            "preaggregation_filters": {
                                "type": "taxon_value",
                                "taxon": "account_id",
                                "value": self._adwords_acc_id,
                                "operator": "=",
                            }
                        },
                        "properties": {"data_sources": ["adwords"]},
                        "taxons": ["fb_adwords_spend_all_required"],
                    }
                ],
                "limit": 100,
            }
        )
        self._husky_context = HuskyQueryContext.from_request(self._blending_request)
        self._info = BlendingQueryInfo.create(self._blending_request, self._husky_context)
        adwords_df = Dataframe(
            create_single_query_mock('adwords'),
            get_mocked_dataframe_columns_map(['adwords|spend']),
            {'adwords_table'},
            {'context'},
        )

        mock__husky_build_query.side_effect = [adwords_df]
        with pytest.raises(HuskyInvalidTelException):
            QueryBuilder.build_query(self._husky_context, self._blending_request, self._info)

    @patch.object(Taxonomy, '_get_filtered_taxons', side_effect=mock_get_taxons)
    @patch('panoramic.cli.husky.service.query_builder.QueryBuilder.build_query')
    def test_required_taxon_in_some_subrequests(self, mock__husky_build_query, mock__get_taxons):
        self._blending_request = BlendingDataRequest(
            {
                "data_subrequests": [
                    {
                        "scope": {
                            "company_id": TEST_COMPANY_ID_50,
                            "preaggregation_filters": {
                                "type": "taxon_value",
                                "taxon": "account_id",
                                "value": self._adwords_acc_id,
                                "operator": "=",
                            },
                        },
                        "properties": {"data_sources": ["adwords"]},
                        "taxons": ["fb_adwords_spend_all_required"],
                    },
                    {
                        "scope": {
                            "company_id": TEST_COMPANY_ID_50,
                            "preaggregation_filters": {
                                "type": "taxon_value",
                                "taxon": "account_id",
                                "value": self._fb_acc_id,
                                "operator": "=",
                            },
                        },
                        "properties": {"data_sources": ["facebook_ads"]},
                        "taxons": ["fb_tw_adwords_spend_all_optional"],
                    },
                ],
                "limit": 100,
            }
        )
        self._husky_context = HuskyQueryContext.from_request(self._blending_request)
        self._info = BlendingQueryInfo.create(self._blending_request, self._husky_context)
        adwords_df = Dataframe(
            create_single_query_mock('adwords'),
            get_mocked_dataframe_columns_map(['adwords|spend']),
            {'adwords_table'},
            {'context'},
        )
        fb_df = Dataframe(
            create_single_query_mock('facebook'),
            get_mocked_dataframe_columns_map(['facebook_ads|spend']),
            {'fb_table'},
            {'context'},
        )

        mock__husky_build_query.side_effect = [adwords_df, fb_df]
        with pytest.raises(HuskyInvalidTelException):
            QueryBuilder.build_query(self._husky_context, self._blending_request, self._info)

    @patch.object(Taxonomy, '_get_filtered_taxons', side_effect=mock_get_taxons)
    @patch('panoramic.cli.husky.service.query_builder.QueryBuilder.build_query')
    def test_basic_dim_calculation(self, mock__husky_build_query, mock__get_taxons):
        self._blending_request = BlendingDataRequest(
            {
                "data_subrequests": [
                    {
                        "scope": {
                            "company_id": TEST_COMPANY_ID_50,
                            "preaggregation_filters": {
                                "type": "taxon_value",
                                "taxon": "account_id",
                                "value": self._twitter_acc_id,
                                "operator": "=",
                            },
                        },
                        "properties": {"data_sources": ["twitter"]},
                    },
                    {
                        "scope": {
                            "company_id": TEST_COMPANY_ID_50,
                            "preaggregation_filters": {
                                "type": "taxon_value",
                                "taxon": "account_id",
                                "value": self._fb_acc_id,
                                "operator": "=",
                            },
                        },
                        "properties": {"data_sources": ["facebook_ads"]},
                    },
                ],
                "taxons": [
                    "fb_tw_merged_objective",
                    "spend",
                    "sumiff_spend_with_merged_objective",
                    "twitter|impressions",
                ],
                "limit": 100,
            }
        )
        self._husky_context = HuskyQueryContext.from_request(self._blending_request)
        self._info = BlendingQueryInfo.create(self._blending_request, self._husky_context)
        twitter_df = Dataframe(
            create_single_query_mock('twitter'),
            get_mocked_dataframe_columns_map(['spend', 'twitter|spend', "twitter|impressions"]),
            {'twitter_table'},
            {'context'},
        )
        fb_df = Dataframe(
            create_single_query_mock('facebook'),
            get_mocked_dataframe_columns_map(['spend', 'facebook_ads|spend']),
            {'fb_table'},
            {'context'},
        )

        mock__husky_build_query.side_effect = [twitter_df, fb_df]
        df = QueryBuilder.build_query(self._husky_context, self._blending_request, self._info)
        self.write_test_expectations('query.sql', compile_query(df.query))
        expected_query = self.read_test_expectations('query.sql')
        self.assertEqual(expected_query, compile_query(df.query))
        # Check that sql templates were correctly generated and passed to husky
        self.assertEqual(
            [
                SqlFormulaTemplate(
                    SqlTemplate('''${twitter|objective}'''),
                    '''__fb_tw_merged_objective2''',
                    'twitter',
                    {'twitter|objective'},
                ),
                SqlFormulaTemplate(
                    SqlTemplate('''${twitter|objective}'''),
                    '''__sumiff_spend_with_merged_objective2''',
                    'twitter',
                    {'twitter|objective'},
                ),
            ],
            mock__husky_build_query.call_args_list[0][0][4],
        )
        self.assertEqual(
            [
                SqlFormulaTemplate(
                    SqlTemplate('''${facebook_ads|objective}'''),
                    '''__fb_tw_merged_objective1''',
                    'facebook_ads',
                    {'facebook_ads|objective'},
                ),
                SqlFormulaTemplate(
                    SqlTemplate('''${facebook_ads|objective}'''),
                    '''__sumiff_spend_with_merged_objective1''',
                    'facebook_ads',
                    {'facebook_ads|objective'},
                ),
            ],
            mock__husky_build_query.call_args_list[1][0][4],
        )

    @patch.object(Taxonomy, '_get_filtered_taxons', side_effect=mock_get_taxons)
    @patch('panoramic.cli.husky.service.query_builder.QueryBuilder.build_query')
    def test_merge_with_concat(self, mock__husky_build_query, mock__get_taxons):
        self._blending_request = BlendingDataRequest(
            {
                "data_subrequests": [
                    {
                        "scope": {
                            "company_id": TEST_COMPANY_ID_50,
                            "preaggregation_filters": {
                                "type": "taxon_value",
                                "taxon": "account_id",
                                "value": self._twitter_acc_id,
                                "operator": "=",
                            },
                        },
                        "properties": {"data_sources": ["twitter"]},
                    },
                    {
                        "scope": {
                            "company_id": TEST_COMPANY_ID_50,
                            "preaggregation_filters": {
                                "type": "taxon_value",
                                "taxon": "account_id",
                                "value": self._fb_acc_id,
                                "operator": "=",
                            },
                        },
                        "properties": {"data_sources": ["facebook_ads"]},
                    },
                ],
                "taxons": ['=d:merge(concat(facebook_ads|objective,"-fb"), concat(twitter|objective,"-tw"))'],
                "limit": 100,
            }
        )
        self._husky_context = HuskyQueryContext.from_request(self._blending_request)
        self._info = BlendingQueryInfo.create(self._blending_request, self._husky_context)
        twitter_df = Dataframe(
            create_single_query_mock('twitter'),
            get_mocked_dataframe_columns_map(['twitter|objective', 'spend', 'twitter|spend', "twitter|impressions"]),
            {'twitter_table'},
            {'context'},
        )
        fb_df = Dataframe(
            create_single_query_mock('facebook'),
            get_mocked_dataframe_columns_map(['facebook_ads|objective', 'spend', 'facebook_ads|spend']),
            {'fb_table'},
            {'context'},
        )

        mock__husky_build_query.side_effect = [twitter_df, fb_df]
        df = QueryBuilder.build_query(self._husky_context, self._blending_request, self._info)
        self.write_test_expectations('query.sql', compile_query(df.query))
        expected_query = self.read_test_expectations('query.sql')
        self.assertEqual(expected_query, compile_query(df.query))

    @patch.object(Taxonomy, '_get_filtered_taxons', side_effect=mock_get_taxons)
    @patch('panoramic.cli.husky.service.query_builder.QueryBuilder.build_query')
    def test_preagg_filter_subrequest_pushdown(self, mock__husky_build_query, mock__get_taxons):
        self._blending_request = BlendingDataRequest(
            {
                "data_subrequests": [
                    {
                        "scope": {
                            "company_id": TEST_COMPANY_ID_50,
                            "preaggregation_filters": {
                                "type": "taxon_value",
                                "taxon": "account_id",
                                "value": self._twitter_acc_id,
                                "operator": "=",
                            },
                        },
                        "properties": {"data_sources": ["twitter"]},
                        "preaggregation_filters": {
                            "type": "taxon_value",
                            "taxon": "fb_tw_merged_objective_concat_xxx",
                            "value": 'views_xxx',
                            "operator": "=",
                        },
                    },
                    {
                        "scope": {
                            "company_id": TEST_COMPANY_ID_50,
                            "preaggregation_filters": {
                                "type": "taxon_value",
                                "taxon": "account_id",
                                "value": self._fb_acc_id,
                                "operator": "=",
                            },
                        },
                        "properties": {"data_sources": ["facebook_ads"]},
                    },
                ],
                "taxons": ['spend'],
                "limit": 100,
            }
        )
        self._husky_context = HuskyQueryContext.from_request(self._blending_request)
        self._info = BlendingQueryInfo.create(self._blending_request, self._husky_context)
        adwords_df = Dataframe(
            create_single_query_mock('twitter'),
            get_mocked_dataframe_columns_map(['spend']),
            {'twitter_table'},
            {'context'},
        )
        fb_df = Dataframe(
            create_single_query_mock('facebook'),
            get_mocked_dataframe_columns_map(['spend']),
            {'fb_table'},
            {'context'},
        )

        mock__husky_build_query.side_effect = [adwords_df, fb_df]
        df = QueryBuilder.build_query(self._husky_context, self._blending_request, self._info)
        self.write_test_expectations('query.sql', compile_query(df.query))
        expected_query = self.read_test_expectations('query.sql')
        self.assertEqual(expected_query, compile_query(df.query))
        #  Check that account filter template is correctly passed to single husky
        self.assertEqual(
            {
                'account_id': SqlFormulaTemplate(
                    SqlTemplate('''${account_id}'''), '''account_id''', 'twitter', {'account_id'}
                ),
                'fb_tw_merged_objective_concat_xxx': SqlFormulaTemplate(
                    SqlTemplate('''concat(${twitter|objective}, 'xxx')'''),
                    '''__fb_tw_merged_objective_concat_xxx1''',
                    'twitter',
                    {'twitter|objective'},
                ),
            },
            mock__husky_build_query.call_args_list[0][1]['filter_templates'],
        )
        self.assertEqual(
            {'account_id': create_sql_formula_template_raw_taxon('account_id', 'facebook_ads')},
            mock__husky_build_query.call_args_list[1][1]['filter_templates'],
        )


class TestTelOverride(BaseTest):
    def _run_no_comparison_test_case(
        self,
        mock__husky_build_query,
        mock__get_by_slugs_list,
        taxons: List[str],
        comparison_config: Optional[ComparisonConfig] = None,
    ):
        mock__get_by_slugs_list.return_value = [
            OverrideMapping(
                slug='om-slug',
                name='my override',
                definition=[('my-val', 'new-val'), (None, 'Another val'), ('orig-val', None)],
                source_type=MappingSourceType.DIRECT,
                created_by='user',
                created_at=now(),
                company_id='company-id',
            )
        ]

        blending_request = BlendingDataRequest(
            {
                "data_subrequests": [
                    {
                        "scope": {
                            "company_id": TEST_COMPANY_ID_50,
                            "preaggregation_filters": {
                                "type": "taxon_value",
                                "taxon": "account_id",
                                "value": 'acc_id_tw123',
                                "operator": "=",
                            },
                        },
                        "properties": {"data_sources": ["twitter"]},
                    },
                    {
                        "scope": {
                            "company_id": TEST_COMPANY_ID_50,
                            "preaggregation_filters": {
                                "type": "taxon_value",
                                "taxon": "account_id",
                                "value": 'acc_id_456',
                                "operator": "=",
                            },
                        },
                        "properties": {"data_sources": ["facebook_ads"]},
                    },
                ],
                "taxons": taxons,
                "limit": 100,
                "comparison": comparison_config,
            }
        )

        husky_context = HuskyQueryContext.from_request(blending_request)
        query_info = BlendingQueryInfo.create(blending_request, husky_context)
        twitter_df = Dataframe(
            create_single_query_mock('twitter'),
            get_mocked_dataframe_columns_map(
                ['twitter|objective', 'objective', 'spend', 'twitter|spend', "twitter|impressions"]
            ),
            {'twitter_table'},
            {'context'},
        )
        fb_df = Dataframe(
            create_single_query_mock('facebook'),
            get_mocked_dataframe_columns_map(['facebook_ads|objective', 'objective', 'spend', 'facebook_ads|spend']),
            {'fb_table'},
            {'context'},
        )

        mock__husky_build_query.side_effect = [twitter_df, fb_df]
        df = QueryBuilder.build_query(husky_context, blending_request, query_info)
        self.write_test_expectations('query.sql', compile_query(df.query))
        expected_query = self.read_test_expectations('query.sql')
        self.assertEqual(expected_query, compile_query(df.query))

    @patch.object(Taxonomy, '_get_filtered_taxons', side_effect=mock_get_taxons)
    @patch('panoramic.cli.husky.core.taxonomy.override_mapping.service.OverrideMappingService.get_by_slugs_list')
    @patch('panoramic.cli.husky.service.query_builder.QueryBuilder.build_query')
    def test_override_simple(self, mock__husky_build_query, mock__get_by_slugs_list, mock__get_taxons):
        self._run_no_comparison_test_case(
            mock__husky_build_query,
            mock__get_by_slugs_list,
            [
                '=d:merge(override(concat(facebook_ads|objective,"-fb"), "om-slug"), override(concat(twitter|objective,"-tw"), "om-slug"))',
                'spend',
            ],
        )

    @patch.object(Taxonomy, '_get_filtered_taxons', side_effect=mock_get_taxons)
    @patch('panoramic.cli.husky.core.taxonomy.override_mapping.service.OverrideMappingService.get_by_slugs_list')
    @patch('panoramic.cli.husky.service.query_builder.QueryBuilder.build_query')
    def test_override_complex(self, mock__husky_build_query, mock__get_by_slugs_list, mock__get_taxons):
        self._run_no_comparison_test_case(
            mock__husky_build_query,
            mock__get_by_slugs_list,
            [
                '=d:override(merge(concat(facebook_ads|objective,"-fb"), concat(twitter|objective,"-tw")), "om-slug")',
                'spend',
            ],
        )

    @patch.object(Taxonomy, '_get_filtered_taxons', side_effect=mock_get_taxons)
    @patch('panoramic.cli.husky.core.taxonomy.override_mapping.service.OverrideMappingService.get_by_slugs_list')
    @patch('panoramic.cli.husky.service.query_builder.QueryBuilder.build_query')
    def test_override_simple_comparisons(self, mock__husky_build_query, mock__get_by_slugs_list, mock__get_taxons):
        mock__get_by_slugs_list.return_value = [
            OverrideMapping(
                slug='om-slug',
                name='my override',
                definition=[('my-val', 'new-val'), (None, 'Another val'), ('orig-val', None)],
                source_type=MappingSourceType.DIRECT,
                created_by='user',
                created_at=now(),
                company_id='company-id',
            )
        ]

        blending_request = BlendingDataRequest(
            {
                "data_subrequests": [
                    {
                        "scope": {
                            "company_id": TEST_COMPANY_ID_50,
                            "preaggregation_filters": {
                                "type": "taxon_value",
                                "taxon": "account_id",
                                "value": 'acc_id_tw123',
                                "operator": "=",
                            },
                        },
                        "properties": {"data_sources": ["twitter"]},
                    },
                    {
                        "scope": {
                            "company_id": TEST_COMPANY_ID_50,
                            "preaggregation_filters": {
                                "type": "taxon_value",
                                "taxon": "account_id",
                                "value": 'acc_id_456',
                                "operator": "=",
                            },
                        },
                        "properties": {"data_sources": ["facebook_ads"]},
                    },
                ],
                "taxons": ['objective', 'cpm'],
                "limit": 100,
                "comparison": ComparisonConfig({"taxons": ["fb_tw_merged_objective_override_unknowns"]}),
            }
        )

        husky_context = HuskyQueryContext.from_request(blending_request)
        query_info = BlendingQueryInfo.create(blending_request, husky_context)
        twitter_df = Dataframe(
            create_single_query_mock('twitter'),
            get_mocked_dataframe_columns_map(
                ['twitter|objective', 'objective', 'spend', 'twitter|spend', "twitter|impressions"]
            ),
            {'twitter_table'},
            {'context'},
        )
        fb_df = Dataframe(
            create_single_query_mock('facebook'),
            get_mocked_dataframe_columns_map(['facebook_ads|objective', 'objective', 'spend', 'facebook_ads|spend']),
            {'fb_table'},
            {'context'},
        )
        twitter_comparison_df = Dataframe(
            create_single_query_mock('twitter_comparison'),
            get_mocked_dataframe_columns_map(['twitter|objective', 'objective', 'spend', 'twitter|spend']),
            {'adwords_comparison_table'},
            {'context'},
        )

        fb_comparison_df = Dataframe(
            create_single_query_mock('facebook_comparison'),
            get_mocked_dataframe_columns_map(['facebook_ads|objective', 'objective', 'spend', 'facebook_ads|spend']),
            {'fb_comparison_table'},
            {'context'},
        )

        mock__husky_build_query.side_effect = [twitter_df, fb_df, twitter_comparison_df, fb_comparison_df]
        df = QueryBuilder.build_query(husky_context, blending_request, query_info)
        self.write_test_expectations('query.sql', compile_query(df.query))
        expected_query = self.read_test_expectations('query.sql')
        self.assertEqual(expected_query, compile_query(df.query))


class TestAggregationDefinitions(BaseTest):
    def setUp(self) -> None:
        super().setUp()
        self._blending_request = BlendingDataRequest(
            {
                "data_subrequests": [
                    {
                        "scope": {
                            "company_id": TEST_COMPANY_ID_50,
                            "preaggregation_filters": {
                                "type": "taxon_value",
                                "taxon": "account_id",
                                "value": "123",
                                "operator": "=",
                            },
                        },
                        "properties": {"data_sources": ["adwords"]},
                    }
                ],
                "limit": 100,
            }
        )
        self._husky_context = HuskyQueryContext.from_request(self._blending_request)
        self._info = BlendingQueryInfo.create(self._blending_request, self._husky_context)

    def _run_test_case(self, taxons, expected_taxons):
        self._blending_request.taxons = taxons
        df = QueryBuilder.build_query(self._husky_context, self._blending_request, self._info)
        self.write_test_expectations('query.sql', compile_query(df.query))
        expected_query = self.read_test_expectations('query.sql')
        assert compile_query(df.query) == expected_query
        assert list(df.slug_to_column.keys()) == expected_taxons

    @patch.object(Taxonomy, '_get_filtered_taxons', side_effect=mock_get_taxons)
    @patch('panoramic.cli.husky.service.model_retriever.component.ModelRetriever.load_models')
    def test_count_all_distinct(self, mock__load_models, mock__get_taxons):
        mock__load_models.return_value = [get_mock_metric_model()]
        self._run_test_case(
            ['account_id', 'simple_count_all', 'simple_count_distinct', 'impressions'],
            ['account_id', 'simple_count_all', 'simple_count_distinct', 'impressions'],
        )

    @patch.object(Taxonomy, '_get_filtered_taxons', side_effect=mock_get_taxons)
    @patch('panoramic.cli.husky.service.model_retriever.component.ModelRetriever.load_models')
    def test_min_max(self, mock__load_models, mock__get_taxons):
        mock__load_models.return_value = [get_mock_metric_model()]
        self._run_test_case(
            ['account_id', 'simple_max', 'simple_min', 'impressions'],
            ['account_id', 'simple_max', 'simple_min', 'impressions'],
        )

    @patch.object(Taxonomy, '_get_filtered_taxons', side_effect=mock_get_taxons)
    @patch('panoramic.cli.husky.service.model_retriever.component.ModelRetriever.load_models')
    def test_first_last_by(self, mock__load_models, mock__get_taxons):
        mock__load_models.return_value = [get_mock_metric_model()]
        self._run_test_case(
            ['simple_first_by', 'simple_last_by', 'impressions'], ['simple_first_by', 'simple_last_by', 'impressions']
        )

    @patch.object(Taxonomy, '_get_filtered_taxons', side_effect=mock_get_taxons)
    @patch('panoramic.cli.husky.service.model_retriever.component.ModelRetriever.load_models')
    def test_use_all_aggs(self, mock__load_models, mock__get_taxons):
        mock__load_models.return_value = [get_mock_metric_model()]
        self._run_test_case(
            [
                'simple_first_by',
                'simple_last_by',
                'simple_min',
                'simple_max',
                'simple_count_all',
                'simple_count_distinct',
                'impressions',
            ],
            [
                'simple_first_by',
                'simple_last_by',
                'simple_min',
                'simple_max',
                'simple_count_all',
                'simple_count_distinct',
                'impressions',
            ],
        )


class TestPhysicalDataSource(BaseTest):
    def setUp(self) -> None:
        super().setUp()
        self._adwords_acc_id = 'acc_id_123'
        self._fb_acc_id = 'acc_id_456'
        self._blending_request = BlendingDataRequest(
            {
                "data_subrequests": [
                    {
                        "scope": {
                            "company_id": TEST_COMPANY_ID_50,
                            "preaggregation_filters": {
                                "type": "taxon_value",
                                "taxon": "account_id",
                                "value": self._adwords_acc_id,
                                "operator": "=",
                            },
                        },
                        "properties": {"data_sources": ["adwords"]},
                        "taxons": ["date", "spend", "cpm"],
                    }
                ],
                "order_by": [{"taxon": "date", "type": "asc"}],
                "limit": 100,
            }
        )

    @patch.object(Taxonomy, '_get_filtered_taxons', side_effect=mock_get_taxons)
    @patch('panoramic.cli.husky.service.query_builder.QueryBuilder.build_query')
    def test_too_many_physical_data_sources(self, mock__husky_build_query, mock__get_taxons):
        self._blending_request.physical_data_sources = ['some_postgres_kycpj', 'SF']

        with pytest.raises(TooManyPhysicalDataSourcesException):
            QueryBuilder.validate_data_request(
                HuskyQueryContext.from_request(self._blending_request), self._blending_request
            )

    @patch.object(Taxonomy, '_get_filtered_taxons', side_effect=mock_get_taxons)
    @patch(
        'panoramic.cli.husky.service.context.HuskyQueryContext.from_request',
        return_value=HuskyQueryContext(HuskyQueryRuntime.bigquery),
    )
    def test_bigquery_output(self, mock__get_taxons, mock__get_source_dialect_by_name):
        self._blending_request.physical_data_sources = ['husky_integration_test_big_query_husky_test_lcurpjg_9']

        with pytest.raises(ModelNotFoundException):
            QueryBuilder.validate_data_request(
                HuskyQueryContext.from_request(self._blending_request), self._blending_request
            )

    @patch.object(Taxonomy, '_get_filtered_taxons', side_effect=mock_get_taxons)
    @patch('panoramic.cli.husky.service.model_retriever.component.ModelRetriever.load_models')
    def test_used_physical_data_sources(self, mock__load_models, mock__get_taxons):
        mock__load_models.return_value = [get_mock_physical_data_sources_model()]

        df = QueryBuilder.validate_data_request(
            HuskyQueryContext.from_request(self._blending_request), self._blending_request
        )
        assert df.used_physical_data_sources == {MOCK_DATA_SOURCE_NAME}
