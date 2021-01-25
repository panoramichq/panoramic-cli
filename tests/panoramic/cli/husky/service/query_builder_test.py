from unittest.mock import patch

from panoramic.cli.husky.core.sql_alchemy_util import compile_query
from panoramic.cli.husky.core.taxonomy.getters import Taxonomy
from panoramic.cli.husky.core.tel.sql_formula import SqlFormulaTemplate, SqlTemplate
from panoramic.cli.husky.service.context import SNOWFLAKE_HUSKY_CONTEXT
from panoramic.cli.husky.service.query_builder import QueryBuilder
from panoramic.cli.husky.service.types.api_data_request_types import InternalDataRequest
from panoramic.cli.husky.service.types.types import QueryInfo
from tests.panoramic.cli.husky.test.mocks.core.taxonomy import (
    TAXON_MAP,
    mock_get_taxons,
)
from tests.panoramic.cli.husky.test.mocks.husky_model import (
    MOCK_DATA_SOURCE_NAME,
    get_mock_entity_model,
    get_mock_metric_model,
)
from tests.panoramic.cli.husky.test.mocks.mock_alchemy_bind_param import (
    mock_alchemy_bind_param,
)
from tests.panoramic.cli.husky.test.test_base import BaseTest


class TestHuskyBuildQuery(BaseTest):
    @patch('panoramic.cli.husky.service.model_retriever.component.ModelRetriever.load_models')
    @patch.object(Taxonomy, '_get_filtered_taxons', side_effect=mock_get_taxons)
    @mock_alchemy_bind_param
    def test_scope_filters(self, mock__get_taxons, mock__load_models):
        mock__load_models.return_value = [
            get_mock_entity_model(),
            get_mock_metric_model(),
        ]
        request = InternalDataRequest(
            {
                'scope': {
                    'project_id': 'project',
                    'company_id': 'company',
                    "preaggregation_filters": {
                        "type": "group",
                        "logical_operator": "AND",
                        "clauses": [
                            {"type": "taxon_value", "taxon": "account_id", "operator": "=", "value": "595126134331606"},
                        ],
                        "negate": False,
                    },
                },
                'properties': {'data_sources': ['mock_data_source']},
                'taxons': ['account_id', 'ad_name'],
            }
        )
        dataframe = QueryBuilder.build_query(
            SNOWFLAKE_HUSKY_CONTEXT,
            request,
            QueryInfo.create(request),
            preloaded_taxons=TAXON_MAP,
        )

        actual = compile_query(dataframe.query)
        self.write_test_expectations('query.sql', actual)
        expected = self.read_test_expectations('query.sql')
        assert expected == actual
        self.assertEqual({'mock_data_source.entity_model'}, dataframe.used_model_names)


class TestHuskyDimFormulas(BaseTest):
    @patch('panoramic.cli.husky.service.model_retriever.component.ModelRetriever.load_models')
    @patch.object(Taxonomy, '_get_filtered_taxons', side_effect=mock_get_taxons)
    @mock_alchemy_bind_param
    def test_simple_concat(self, mock__get_taxons, mock__load_models):
        mock__load_models.return_value = [
            get_mock_entity_model(),
            get_mock_metric_model(),
        ]
        request = InternalDataRequest(
            {
                'scope': {
                    'project_id': 'project-id',
                    'company_id': 'company-id',
                    "preaggregation_filters": {
                        "type": "taxon_value",
                        "taxon": "account_id",
                        "operator": "=",
                        "value": "abc",
                    },
                },
                'properties': {'data_sources': ['mock_data_source']},
                'taxons': ['account_id', 'ad_name'],
            }
        )
        dimension_templates = [
            SqlFormulaTemplate(
                SqlTemplate('''concat(${ad_name},'xx')'''), '''__1''', MOCK_DATA_SOURCE_NAME, {'ad_name'}
            )
        ]
        df = QueryBuilder.build_query(
            SNOWFLAKE_HUSKY_CONTEXT,
            request,
            QueryInfo.create(request),
            preloaded_taxons=TAXON_MAP,
            dimension_templates=dimension_templates,
        )
        actual = compile_query(df.query)
        self.write_test_expectations('query.sql', actual)
        expected = self.read_test_expectations('query.sql')
        assert expected == actual
        self.assertEqual({'mock_data_source.entity_model'}, df.used_model_names)
