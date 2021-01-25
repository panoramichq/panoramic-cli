import pytest

from panoramic.cli.husky.core.tel.evaluator.ast_features import (
    can_become_comparison_metric,
)
from panoramic.cli.husky.core.tel.evaluator.context import TelRootContext, node_id_maker
from panoramic.cli.husky.core.tel.tel_dialect import TaxonTelDialect
from panoramic.cli.husky.service.context import SNOWFLAKE_HUSKY_CONTEXT


@pytest.mark.parametrize(
    'expression, expected',
    [
        ('spend/impressions', True),
        ('1 / spend', True),
        ('(1 / spend) / ( 10 - 3)', True),
        ('(1 / 20) / ( 10 - 3)', False),
        ('1 / (20 - spend)', True),
        ('1 + spend * 10', False),
        ('1 - spend / (20 * 10)', False),
        ('1 / to_number(20)', False),
        ('1 / to_number(ad_id)', True),
        ('1 / 30 / spend', True),
    ],
)
def test_detection(expression, expected):
    tel_dialect = TaxonTelDialect()
    context = TelRootContext(SNOWFLAKE_HUSKY_CONTEXT, tel_dialect, None, {}, next_node_id=node_id_maker())
    node = tel_dialect.visit(expression, context, skip_root_node=True)

    res = can_become_comparison_metric(node)

    assert res == expected
