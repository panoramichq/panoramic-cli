import pytest

from panoramic.cli.husky.core.tel.evaluator.context import TelRootContext, node_id_maker
from panoramic.cli.husky.core.tel.tel_dialect import TaxonTelDialect
from panoramic.cli.husky.service.context import SNOWFLAKE_HUSKY_CONTEXT
from tests.panoramic.cli.husky.test.mocks.core.taxonomy import TAXON_MAP


def execute_test(test_input, expected):
    tel_dialect = TaxonTelDialect()
    context = TelRootContext(SNOWFLAKE_HUSKY_CONTEXT, tel_dialect, None, TAXON_MAP, node_id_maker())
    result = tel_dialect.visit(test_input, context)

    assert expected == result.literal_value(context)


@pytest.mark.parametrize(
    'input, expected',
    [
        ('5', 5),
        ('3 + 2', 5),
        ('6 == 4', False),
        ('taxon', None),
        ('coalesce(taxon, 0)', None),
        ('(1 - 0)', 1),
        ('override(gender, "our-gender-mapping", false)', None),
    ],
)
def test_literals(input, expected):
    execute_test(input, expected)
