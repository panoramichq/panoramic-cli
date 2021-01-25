from panoramic.cli.husky.core.sql_alchemy_util import compile_query
from panoramic.cli.husky.core.taxonomy.enums import TaxonOrderType
from panoramic.cli.husky.service.context import SNOWFLAKE_HUSKY_CONTEXT
from panoramic.cli.husky.service.filter_builder.enums import (
    FilterClauseType,
    SimpleFilterOperator,
)
from panoramic.cli.husky.service.filter_builder.filter_clauses import (
    TaxonValueFilterClause,
)
from panoramic.cli.husky.service.graph_builder.component import GraphBuilder
from panoramic.cli.husky.service.projection_builder.component import ProjectionBuilder
from panoramic.cli.husky.service.select_builder.component import SelectBuilder
from panoramic.cli.husky.service.types.api_data_request_types import (
    DataRequestProperties,
    TaxonDataOrder,
)
from panoramic.cli.husky.service.types.api_scope_types import Scope
from tests.panoramic.cli.husky.test.mocks.core.taxonomy import (
    get_specific_select_mocked_taxons,
)
from tests.panoramic.cli.husky.test.mocks.husky_model import (
    MOCK_DATA_SOURCE_NAME,
    get_mock_entity_model,
    get_mock_metric_gender_model,
    get_mock_metric_model,
)
from tests.panoramic.cli.husky.test.mocks.mock_alchemy_bind_param import (
    mock_alchemy_bind_param,
)
from tests.panoramic.cli.husky.test.test_base import BaseTest


class TestProjectionBuilder(BaseTest):
    def setUp(self) -> None:
        super().setUp()
        self._scope = Scope(dict(company_id='10', project_id='10'))
        self._properties = DataRequestProperties(dict(data_sources=[MOCK_DATA_SOURCE_NAME]))
        self._graph = GraphBuilder(
            [get_mock_entity_model(), get_mock_metric_model(), get_mock_metric_gender_model()]
        ).build_graph()

    @mock_alchemy_bind_param
    def test_basic_build_query_with_order_by(self):
        selected_taxons = ['impressions', 'ad_id']
        query, taxon_model_info_map, _ = SelectBuilder(
            SNOWFLAKE_HUSKY_CONTEXT,
            self._scope,
            get_specific_select_mocked_taxons(selected_taxons),
            get_specific_select_mocked_taxons(selected_taxons),
            self._graph,
            'data-source',
        ).get_query()

        taxon_order_1 = TaxonDataOrder({'taxon': 'impressions', 'type': TaxonOrderType.desc.value})
        taxon_order_2 = TaxonDataOrder({'taxon': 'ad_id', 'type': TaxonOrderType.asc.value})
        order_by = [taxon_order_1, taxon_order_2]

        final_dataframe = ProjectionBuilder.query(
            query,
            taxon_model_info_map,
            get_specific_select_mocked_taxons(selected_taxons),
            'data-source',
            order_by,
            1,
            2,
            {'context'},
        )

        self.write_test_expectations('query.sql', compile_query(final_dataframe.query))
        expected = self.read_test_expectations('query.sql')

        self.assertEqual(expected, compile_query(final_dataframe.query))

    @mock_alchemy_bind_param
    def test_basic_build_query_with_pre_filter(self):
        projected_taxons = ['impressions', 'spend']

        pre_filter = TaxonValueFilterClause(
            {
                'type': FilterClauseType.TAXON_VALUE.value,
                'taxon': 'ad_name',
                'operator': SimpleFilterOperator.LIKE,
                'value': 'zombies!',
            }
        )

        selected_taxons = projected_taxons + ['ad_name']

        query, taxon_model_info_map, _ = SelectBuilder(
            SNOWFLAKE_HUSKY_CONTEXT,
            self._scope,
            get_specific_select_mocked_taxons(selected_taxons),
            get_specific_select_mocked_taxons(projected_taxons),
            self._graph,
            'data-source',
            pre_filter,
        ).get_query()

        final_dataframe = ProjectionBuilder.query(
            query,
            taxon_model_info_map,
            get_specific_select_mocked_taxons(projected_taxons),
            'data-source',
            None,
            None,
            None,
            {'context'},
        )

        self.write_test_expectations('query.sql', compile_query(final_dataframe.query))
        expected = self.read_test_expectations('query.sql')

        self.assertEqual(expected, compile_query(final_dataframe.query))

    def _run_basic_test(self, projection_taxons, selected_taxons=None):
        if selected_taxons is None:
            selected_taxons = projection_taxons

        query, taxon_model_info_map, _ = SelectBuilder(
            SNOWFLAKE_HUSKY_CONTEXT,
            self._scope,
            get_specific_select_mocked_taxons(selected_taxons),
            get_specific_select_mocked_taxons(projection_taxons),
            self._graph,
            'data-source',
        ).get_query()

        final_dataframe = ProjectionBuilder.query(
            query,
            taxon_model_info_map,
            get_specific_select_mocked_taxons(projection_taxons),
            'data-source',
            None,
            None,
            None,
            {'context'},
        )

        self.write_test_expectations('query.sql', compile_query(final_dataframe.query))
        expected = self.read_test_expectations('query.sql')

        assert compile_query(final_dataframe.query) == expected

    @mock_alchemy_bind_param
    def test_basic_build_query(self):
        self._run_basic_test(['impressions', 'ad_id'])

    @mock_alchemy_bind_param
    def test_basic_build_query_counts(self):
        self._run_basic_test(['ad_id', 'account_id', 'simple_count_all', 'simple_count_distinct'])

    @mock_alchemy_bind_param
    def test_basic_build_query_min_max(self):
        self._run_basic_test(['ad_id', 'simple_min', 'simple_max'])

    @mock_alchemy_bind_param
    def test_basic_build_query_first_last_by(self):
        self._run_basic_test(
            ['ad_id', 'simple_first_by', 'simple_last_by'], ['ad_id', 'simple_first_by', 'simple_last_by', 'objective']
        )
