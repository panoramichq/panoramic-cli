from sqlalchemy import literal_column, select

from panoramic.cli.husky.core.model.enums import ValueQuantityType
from panoramic.cli.husky.core.sql_alchemy_util import compile_query
from panoramic.cli.husky.service.context import SNOWFLAKE_HUSKY_CONTEXT
from panoramic.cli.husky.service.filter_builder.enums import (
    FilterClauseType,
    SimpleFilterOperator,
)
from panoramic.cli.husky.service.filter_builder.filter_clauses import (
    TaxonValueFilterClause,
)
from panoramic.cli.husky.service.select_builder.scope_guard import ScopeGuard
from panoramic.cli.husky.service.select_builder.taxon_model_info import TaxonModelInfo
from panoramic.cli.husky.service.types.api_scope_types import Scope
from tests.panoramic.cli.husky.test.mocks.husky_model import get_mock_metric_model
from tests.panoramic.cli.husky.test.test_base import BaseTest


class TestScopeGuard(BaseTest):
    def test_scope_filters(self):
        scope_filter = TaxonValueFilterClause(
            {
                'type': FilterClauseType.TAXON_VALUE.value,
                'taxon': 'account_id',
                'operator': SimpleFilterOperator.EQ.value,
                'value': '10',
            }
        ).to_native()
        scope = Scope(dict(company_id='10', project_id='10', preaggregation_filters=scope_filter))
        model = get_mock_metric_model()
        query = select([literal_column('test')])
        model_info = TaxonModelInfo('acc_id_column', model.name, ValueQuantityType.scalar)
        new_query = ScopeGuard.add_scope_row_filters(SNOWFLAKE_HUSKY_CONTEXT, scope, query, dict(account_id=model_info))
        # Not global model, we are fine without scope filters
        assert compile_query(new_query) == "SELECT test \nWHERE acc_id_column = '10'"
