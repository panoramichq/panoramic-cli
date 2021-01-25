import pytest

from panoramic.cli.husky.core.enums import DbDataType
from panoramic.cli.husky.core.federated.model.models import FdqModelAttribute
from panoramic.cli.husky.core.federated.model.tel.data_structures import (
    AttributeValidationTelVisitorParams,
)
from panoramic.cli.husky.core.federated.model.tel.visitor import (
    AttributeValidationTelVisitor,
)
from panoramic.cli.husky.core.tel.exceptions import TelExpressionException
from panoramic.cli.husky.core.tel.tel_dialect import ModelTelDialect

_TMP_MODEL_ATTRIBUTES = [
    FdqModelAttribute(data_type=DbDataType.CHARACTER_VARYING, data_reference='"column_0"', field_map=['source_taxon']),
    FdqModelAttribute(data_type=DbDataType.CHARACTER_VARYING, data_reference='"column_1"', field_map=['gender']),
    FdqModelAttribute(data_type=DbDataType.CHARACTER_VARYING, data_reference='"column_2"', field_map=['ad_id']),
    FdqModelAttribute(data_type=DbDataType.INTEGER, data_reference='"column_3"', field_map=['spend']),
    FdqModelAttribute(data_type=DbDataType.INTEGER, data_reference='spend + 10', field_map=['spend_2']),
]


@pytest.mark.parametrize(
    ['inp', 'exp', 'exp_used_taxons', 'exp_used_columns'],
    [
        ('1+10', '1+10', set(), set()),
        ('1+10/spend', '1+10/"column_3"', {'spend'}, {'column_3'}),
        ('1+10/spend_2', '1+10/"column_3"+10', {'spend_2', 'spend'}, {'column_3'}),
        (
            '1+10/spend_2 * unknown_taxon',
            '1+10/"column_3"+10*unknown_taxon',
            {'spend_2', 'spend', 'unknown_taxon'},
            {'column_3'},
        ),
        ('1 + fn(4, "column_3", spend)', '1+fn(4, "column_3", "column_3")', {'spend'}, {'column_3'}),
    ],
)
def test_validator_visitor_success(inp, exp, exp_used_taxons, exp_used_columns):
    visitor_params = AttributeValidationTelVisitorParams('source_taxon', _TMP_MODEL_ATTRIBUTES)
    tree = ModelTelDialect.parse(inp)
    visitor = AttributeValidationTelVisitor(visitor_params)
    visitor.visit(tree)
    assert visitor.result.result_expression == exp
    assert visitor.result.used_taxon_slugs == exp_used_taxons
    assert visitor.result.used_column_names == exp_used_columns


@pytest.mark.parametrize(
    ['inp', 'exp'],
    [
        ('1+', 'Unexpected symbol "<EOF>". Occurred at position 3, line 1 in expression "1+"'),
        ('fn(,)', 'Unexpected symbol ")". Occurred at position 5, line 1 in expression "fn(,)"'),
        ('fn(1, )', 'Unexpected symbol ")". Occurred at position 7, line 1 in expression "fn(1, )"'),
    ],
)
def test_validator_visitor_format_fail(inp, exp):
    with pytest.raises(TelExpressionException) as exception_info:
        visitor_params = AttributeValidationTelVisitorParams('source_taxon', _TMP_MODEL_ATTRIBUTES)
        tree = ModelTelDialect.parse(inp)
        visitor = AttributeValidationTelVisitor(visitor_params)
        visitor.visit(tree)

    assert exception_info.value.args[0] == exp
