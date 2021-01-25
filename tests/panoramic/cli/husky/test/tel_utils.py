from snowflake.sqlalchemy.snowdialect import SnowflakeDialect

from panoramic.cli.husky.core.sql_alchemy_util import compile_query
from panoramic.cli.husky.core.tel.result import ExprResult

_DIALECT = SnowflakeDialect()


class ExprException:
    msg: str

    def __init__(self, msg):
        self.msg = msg


def get_test_case_id(val):
    if isinstance(val, ExprException):
        return 'error'
    elif isinstance(val, ExprResult):
        return 'success'
    else:
        # TEL definition
        id_parts = [val[0]]

        # set with VDS
        if len(val[1]):
            id_parts.append('|'.join(sorted(val[1])))
        else:
            id_parts.append('<no-vds>')

        # optional definition of taxon type
        if len(val) == 3:
            id_parts.append(val[2])

        return ';'.join(id_parts)


def assert_result_formulas(actual_result: ExprResult, expected_result: ExprResult):
    assert (
        actual_result.data_source_formula_templates == expected_result.data_source_formula_templates
    ), 'data_source_formulas dont match'
    assert list(map(repr, actual_result.dimension_formulas)) == list(
        map(repr, expected_result.dimension_formulas)
    ), 'dimension_formulas dont match'
    assert list(map(repr, actual_result.pre_formulas)) == list(
        map(repr, expected_result.pre_formulas)
    ), 'pre_formulas dont match'
    assert compile_query(actual_result.sql(_DIALECT), _DIALECT) == compile_query(
        expected_result.sql(_DIALECT), _DIALECT
    ), 'sql dont match'
    assert actual_result.phase == expected_result.phase, 'phase dont match'
    assert actual_result.override_mappings == expected_result.override_mappings, 'override mappings dont match'

    assert actual_result.invalid_value == expected_result.invalid_value, "invalid_value doesn't match"
