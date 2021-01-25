from panoramic.cli.husky.core.sql_alchemy_util import compile_query
from panoramic.cli.husky.service.context import SNOWFLAKE_HUSKY_CONTEXT
from panoramic.cli.husky.service.filter_builder.enums import (
    FilterClauseType,
    SimpleFilterOperator,
)
from panoramic.cli.husky.service.filter_builder.filter_clauses import (
    TaxonTaxonFilterClause,
    TaxonValueFilterClause,
)
from panoramic.cli.husky.service.graph_builder.component import GraphBuilder
from panoramic.cli.husky.service.select_builder.component import SelectBuilder
from panoramic.cli.husky.service.types.api_scope_types import Scope
from tests.panoramic.cli.husky.test.mocks.core.taxonomy import (
    get_specific_select_mocked_taxons,
)
from tests.panoramic.cli.husky.test.mocks.husky_model import (
    MOCK_DATA_SOURCE_NAME,
    get_mock_entity_model,
    get_mock_metric_gender_model,
    get_mock_metric_model,
    get_mock_metric_time_taxon_model,
)
from tests.panoramic.cli.husky.test.mocks.mock_alchemy_bind_param import (
    mock_alchemy_bind_param,
)
from tests.panoramic.cli.husky.test.test_base import BaseTest


class TestSelectBuilder(BaseTest):
    def setUp(self):
        super().setUp()
        self.graph = GraphBuilder(
            [
                get_mock_entity_model(),
                get_mock_metric_model(),
                get_mock_metric_gender_model(),
            ]
        ).build_graph()
        self.scope = Scope(dict(company_id='10', project_id='10'))

    @mock_alchemy_bind_param
    def test_basic_build_query(self):
        result, _, effectively_used_models = SelectBuilder(
            SNOWFLAKE_HUSKY_CONTEXT,
            self.scope,
            get_specific_select_mocked_taxons(['impressions', 'ad_id']),
            get_specific_select_mocked_taxons(['impressions', 'ad_id']),
            self.graph,
            'data-source',
        ).get_query()

        self.write_test_expectations('query.sql', compile_query(result))
        expected = self.read_test_expectations('query.sql')
        self.assertEqual(expected, compile_query(result))
        self.assertDictEqual(
            {'noncached_models': ['mock_data_source.metric_model']},
            effectively_used_models.to_primitive(),
        )

    @mock_alchemy_bind_param
    def test_query_with_pre_filter_with_filter_taxon_selected(self):
        filter_clause = TaxonValueFilterClause(
            {
                'type': FilterClauseType.TAXON_VALUE.value,
                'taxon': 'ad_name',
                'operator': SimpleFilterOperator.LIKE.value,
                'value': '%abc%',
            }
        )

        result, _, _ = SelectBuilder(
            SNOWFLAKE_HUSKY_CONTEXT,
            self.scope,
            get_specific_select_mocked_taxons(['impressions', 'ad_id', 'ad_name', filter_clause.taxon]),
            get_specific_select_mocked_taxons(['impressions', 'ad_id', 'ad_name']),
            self.graph,
            'data-source',
            filter_clause=filter_clause,
        ).get_query()

        self.write_test_expectations('query.sql', compile_query(result))
        expected = self.read_test_expectations('query.sql')

        self.assertEqual(expected, compile_query(result).strip())

    @mock_alchemy_bind_param
    def test_query_with_pre_filter_without_filter_taxons(self):
        filter_clause = TaxonValueFilterClause(
            {
                'type': FilterClauseType.TAXON_VALUE.value,
                'taxon': 'ad_name',
                'operator': SimpleFilterOperator.LIKE.value,
                'value': '%abc%',
            }
        )

        result, _, _ = SelectBuilder(
            SNOWFLAKE_HUSKY_CONTEXT,
            self.scope,
            get_specific_select_mocked_taxons(['impressions', 'ad_id', filter_clause.taxon]),
            get_specific_select_mocked_taxons(['impressions', 'ad_id']),
            self.graph,
            'data-source',
            filter_clause=filter_clause,
        ).get_query()

        self.write_test_expectations('query.sql', compile_query(result))
        expected = self.read_test_expectations('query.sql')

        self.assertEqual(expected, compile_query(result).strip())

    @mock_alchemy_bind_param
    def test_query_with_pre_taxon_taxon_pre_filter(self):
        filter_clause = TaxonTaxonFilterClause(
            {
                'type': FilterClauseType.TAXON_VALUE.value,
                'taxon': 'spend',
                'right_taxon': 'impressions',
                'operator': SimpleFilterOperator.EQ.value,
            }
        )

        result, _, _ = SelectBuilder(
            SNOWFLAKE_HUSKY_CONTEXT,
            self.scope,
            get_specific_select_mocked_taxons(['ad_id'] + [s for s in filter_clause.get_taxon_slugs()]),
            get_specific_select_mocked_taxons(['ad_id']),
            self.graph,
            'data-source',
            filter_clause=filter_clause,
        ).get_query()

        self.write_test_expectations('query.sql', compile_query(result))
        expected = self.read_test_expectations('query.sql')

        self.assertEqual(expected, compile_query(result).strip())

    @mock_alchemy_bind_param
    def test_basic_build_join_query(self):
        taxons = get_specific_select_mocked_taxons(['spend', 'gender', 'impressions', 'ad_id', 'ad_name'])

        result, _, effectively_used_models = SelectBuilder(
            SNOWFLAKE_HUSKY_CONTEXT, self.scope, taxons, taxons, self.graph, 'data-source'
        ).get_query()
        self.write_test_expectations('query.sql', compile_query(result))
        expected = self.read_test_expectations('query.sql')

        self.assertEqual(expected, compile_query(result))
        self.assertDictEqual(
            {
                'noncached_models': ['mock_data_source.metric_gender_model', 'mock_data_source.entity_model'],
            },
            effectively_used_models.to_primitive(),
        )

    @mock_alchemy_bind_param
    def test_gender_build_query(self):
        taxons = get_specific_select_mocked_taxons(['spend', 'gender', 'impressions', 'ad_id'])

        result, _, _ = SelectBuilder(
            SNOWFLAKE_HUSKY_CONTEXT, self.scope, taxons, taxons, self.graph, 'data-source'
        ).get_query()
        self.write_test_expectations('query.sql', compile_query(result))
        expected = self.read_test_expectations('query.sql')

        self.assertEqual(expected, compile_query(result))

    @mock_alchemy_bind_param
    def test_namespaced_dimension_build_query(self):
        result, _, effectively_used_models = SelectBuilder(
            SNOWFLAKE_HUSKY_CONTEXT,
            self.scope,
            get_specific_select_mocked_taxons(['impressions', 'ad_id', f'{MOCK_DATA_SOURCE_NAME}|dimension']),
            get_specific_select_mocked_taxons(['impressions', 'ad_id', f'{MOCK_DATA_SOURCE_NAME}|dimension']),
            self.graph,
            'data-source',
        ).get_query()

        self.write_test_expectations('query.sql', compile_query(result))
        expected = self.read_test_expectations('query.sql')

        self.assertEqual(expected, compile_query(result))
        self.assertDictEqual(
            {
                'noncached_models': ['mock_data_source.metric_model', 'mock_data_source.entity_model'],
            },
            effectively_used_models.to_primitive(),
        )

    @mock_alchemy_bind_param
    def test_namespaced_taxons_build_query(self):
        result, _, effectively_used_models = SelectBuilder(
            SNOWFLAKE_HUSKY_CONTEXT,
            self.scope,
            get_specific_select_mocked_taxons(
                ['impressions', 'ad_id', f'{MOCK_DATA_SOURCE_NAME}|dimension', f'{MOCK_DATA_SOURCE_NAME}|metric']
            ),
            get_specific_select_mocked_taxons(
                ['impressions', 'ad_id', f'{MOCK_DATA_SOURCE_NAME}|dimension', f'{MOCK_DATA_SOURCE_NAME}|metric']
            ),
            self.graph,
            'data-source',
        ).get_query()

        self.write_test_expectations('query.sql', compile_query(result))
        expected = self.read_test_expectations('query.sql')

        self.assertEqual(expected, compile_query(result))
        self.assertDictEqual(
            {
                'noncached_models': ['mock_data_source.metric_model', 'mock_data_source.entity_model'],
            },
            effectively_used_models.to_primitive(),
        )


class TestSelectBuilderTimeTaxon(BaseTest):
    def setUp(self):
        super().setUp()
        self.graph = GraphBuilder(
            [get_mock_entity_model(), get_mock_metric_model(), get_mock_metric_time_taxon_model()]
        ).build_graph()
        self.scope = Scope(dict(company_id='10', project_id='10'))

    @mock_alchemy_bind_param
    def test_basic_build_join_query(self):
        taxons = get_specific_select_mocked_taxons(['spend', 'impressions', 'ad_id', 'ad_name', 'week_of_year'])

        result, _, _ = SelectBuilder(
            SNOWFLAKE_HUSKY_CONTEXT, self.scope, taxons, taxons, self.graph, 'data-source'
        ).get_query()
        self.write_test_expectations('query.sql', compile_query(result))
        expected = self.read_test_expectations('query.sql')

        self.assertEqual(expected, compile_query(result))
