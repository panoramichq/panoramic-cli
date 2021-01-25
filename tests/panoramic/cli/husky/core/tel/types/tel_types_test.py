from enum import Enum
from typing import Any, Union

import pytest

from panoramic.cli.husky.core.taxonomy.enums import TaxonTypeEnum, ValidationType
from panoramic.cli.husky.core.tel.exceptions import TelExpressionException
from panoramic.cli.husky.core.tel.tel_dialect import TaxonTelDialect
from panoramic.cli.husky.core.tel.types.tel_types import TelDataType, TelType
from panoramic.cli.husky.service.context import SNOWFLAKE_HUSKY_CONTEXT
from tests.panoramic.cli.husky.test.mocks.core.taxonomy import TAXON_MAP


class ExceptionResult:
    message: str

    def __init__(self, message: str):
        self.message = message


def generate_test_id(test_input: Any) -> str:
    if isinstance(test_input, Enum):
        return test_input.name
    if isinstance(test_input, TelType):
        return f'success({test_input.data_type.name},constant={test_input.is_constant})'
    if isinstance(test_input, ExceptionResult):
        return 'exception'
    return str(test_input)


def execute_test(tel_expression: str, taxon_type: TaxonTypeEnum, expected_result: Union[TelType, ExceptionResult]):
    if isinstance(expected_result, TelType):
        # Expecting success
        result = TaxonTelDialect().render(
            tel_expression, SNOWFLAKE_HUSKY_CONTEXT, taxon_map=TAXON_MAP, taxon_type=taxon_type
        )
        tel_type = result.return_type

        assert tel_type.data_type == expected_result.data_type, "TelType.data_type doesn't match"
        assert tel_type.is_constant == expected_result.is_constant, "TelType.is_constant doesn't match"
    elif isinstance(expected_result, ExceptionResult):
        # Expecting exception
        with pytest.raises(TelExpressionException) as exception_info:
            TaxonTelDialect().render(
                tel_expression, SNOWFLAKE_HUSKY_CONTEXT, taxon_map=TAXON_MAP, taxon_type=taxon_type
            )
        # Extract the first part of the error message
        data_type_error_message = exception_info.value.args[0].split('.')[0]

        assert data_type_error_message == expected_result.message
    else:
        raise AssertionError(f'Unknown type of the expected result: {type(expected_result)}')


def test_tel_type_data_type():
    assert TelType(TelDataType.STRING).is_string()
    assert TelType(TelDataType.INTEGER).is_integer()
    assert TelType(TelDataType.INTEGER).is_number()
    assert TelType(TelDataType.NUMERIC).is_number()
    assert TelType(TelDataType.DATETIME).is_date()
    assert TelType(TelDataType.DATETIME).is_datetime()
    assert TelType(TelDataType.BOOLEAN).is_boolean()


def test_tel_type_constant():
    assert TelType(TelDataType.ANY, is_constant=True).is_constant
    assert not TelType(TelDataType.ANY, is_constant=False).is_constant


def test_tel_type_copy():
    tel_type = TelType(TelDataType.ANY, is_constant=False)

    assert tel_type.copy().data_type is TelDataType.ANY
    assert tel_type.copy().is_constant is False

    assert tel_type.copy(data_type=TelDataType.STRING).data_type is TelDataType.STRING
    assert tel_type.copy(data_type=TelDataType.STRING).is_constant is False

    assert tel_type.copy(is_constant=True).data_type is TelDataType.ANY
    assert tel_type.copy(is_constant=True).is_constant is True

    assert tel_type.copy(data_type=TelDataType.STRING, is_constant=True).data_type is TelDataType.STRING
    assert tel_type.copy(data_type=TelDataType.STRING, is_constant=True).is_constant is True


def test_tel_type_from_validation_type():
    tel_type = TelType.from_taxon_validation_type(ValidationType.text)

    assert tel_type.data_type is TelDataType.STRING
    assert tel_type.is_constant is False


@pytest.mark.parametrize(
    "tel_types,expected_common_type",
    [
        ([], TelType(TelDataType.UNKNOWN)),
        ([TelType(TelDataType.UNKNOWN)], TelType(TelDataType.UNKNOWN)),
        ([TelType(TelDataType.UNKNOWN), TelType(TelDataType.UNKNOWN)], TelType(TelDataType.UNKNOWN)),
        ([TelType(TelDataType.UNKNOWN), TelType(TelDataType.ANY)], TelType(TelDataType.UNKNOWN)),
        ([TelType(TelDataType.UNKNOWN), TelType(TelDataType.STRING)], TelType(TelDataType.UNKNOWN)),
        ([TelType(TelDataType.NONE_OPTIONAL)], TelType(TelDataType.ANY)),
        ([TelType(TelDataType.NONE_OPTIONAL), TelType(TelDataType.NONE_OPTIONAL)], TelType(TelDataType.ANY)),
        ([TelType(TelDataType.NONE_OPTIONAL), TelType(TelDataType.ANY)], TelType(TelDataType.ANY)),
        ([TelType(TelDataType.NONE_OPTIONAL), TelType(TelDataType.STRING)], TelType(TelDataType.STRING)),
        (
            [TelType(TelDataType.NONE_OPTIONAL), TelType(TelDataType.STRING), TelType(TelDataType.INTEGER)],
            TelType(TelDataType.UNKNOWN),
        ),
        (
            [TelType(TelDataType.NONE_OPTIONAL), TelType(TelDataType.NUMERIC), TelType(TelDataType.INTEGER)],
            TelType(TelDataType.NUMERIC),
        ),
        ([TelType(TelDataType.ANY)], TelType(TelDataType.ANY)),
        ([TelType(TelDataType.ANY), TelType(TelDataType.ANY)], TelType(TelDataType.ANY)),
        ([TelType(TelDataType.ANY), TelType(TelDataType.STRING)], TelType(TelDataType.STRING)),
        (
            [TelType(TelDataType.ANY), TelType(TelDataType.STRING), TelType(TelDataType.INTEGER)],
            TelType(TelDataType.UNKNOWN),
        ),
        (
            [TelType(TelDataType.ANY), TelType(TelDataType.NUMERIC), TelType(TelDataType.INTEGER)],
            TelType(TelDataType.NUMERIC),
        ),
        ([TelType(TelDataType.STRING)], TelType(TelDataType.STRING)),
        ([TelType(TelDataType.STRING), TelType(TelDataType.STRING)], TelType(TelDataType.STRING)),
        ([TelType(TelDataType.STRING), TelType(TelDataType.INTEGER)], TelType(TelDataType.UNKNOWN)),
        ([TelType(TelDataType.NUMERIC), TelType(TelDataType.INTEGER)], TelType(TelDataType.NUMERIC)),
        (
            [TelType(TelDataType.STRING, is_constant=True), TelType(TelDataType.ANY, is_constant=True)],
            TelType(TelDataType.STRING, is_constant=True),
        ),
        (
            [TelType(TelDataType.STRING, is_constant=True), TelType(TelDataType.STRING, is_constant=True)],
            TelType(TelDataType.STRING, is_constant=True),
        ),
        (
            [TelType(TelDataType.STRING, is_constant=True), TelType(TelDataType.STRING, is_constant=False)],
            TelType(TelDataType.STRING, is_constant=False),
        ),
        (
            [TelType(TelDataType.NUMERIC, is_constant=True), TelType(TelDataType.INTEGER, is_constant=False)],
            TelType(TelDataType.NUMERIC, is_constant=False),
        ),
    ],
)
def test_tel_type_return_common_type(tel_types, expected_common_type):
    assert TelType.return_common_type(tel_types).data_type is expected_common_type.data_type
    assert TelType.return_common_type(tel_types).is_constant is expected_common_type.is_constant


@pytest.mark.parametrize(
    "tel_types,expected_compatibility",
    [
        ([], False),
        ([TelType(TelDataType.UNKNOWN)], False),
        ([TelType(TelDataType.UNKNOWN), TelType(TelDataType.UNKNOWN)], False),
        ([TelType(TelDataType.UNKNOWN), TelType(TelDataType.ANY)], False),
        ([TelType(TelDataType.UNKNOWN), TelType(TelDataType.STRING)], False),
        ([TelType(TelDataType.ANY)], True),
        ([TelType(TelDataType.ANY), TelType(TelDataType.ANY)], True),
        ([TelType(TelDataType.ANY), TelType(TelDataType.STRING)], True),
        ([TelType(TelDataType.ANY), TelType(TelDataType.STRING), TelType(TelDataType.INTEGER)], False),
        ([TelType(TelDataType.ANY), TelType(TelDataType.NUMERIC), TelType(TelDataType.INTEGER)], True),
        ([TelType(TelDataType.STRING)], True),
        ([TelType(TelDataType.STRING), TelType(TelDataType.STRING)], True),
        ([TelType(TelDataType.STRING), TelType(TelDataType.INTEGER)], False),
        ([TelType(TelDataType.NUMERIC), TelType(TelDataType.INTEGER)], True),
    ],
)
def test_tel_type_compatible_data_types(tel_types, expected_compatibility):
    assert TelType.are_compatible_data_types(tel_types) is expected_compatibility


@pytest.mark.parametrize(
    "tel_expression,expected_result",
    [
        ("'str'", TelType(TelDataType.STRING, is_constant=True)),
        ('"str"', TelType(TelDataType.STRING, is_constant=True)),
        ("-42", TelType(TelDataType.INTEGER, is_constant=True)),
        ("4.2", TelType(TelDataType.NUMERIC, is_constant=True)),
        ("true", TelType(TelDataType.BOOLEAN, is_constant=True)),
        ("false", TelType(TelDataType.BOOLEAN, is_constant=True)),
    ],
    ids=generate_test_id,
)
def test_constant_expressions(tel_expression, expected_result):
    execute_test(tel_expression, TaxonTypeEnum.dimension, expected_result)


@pytest.mark.parametrize(
    "tel_expression,taxon_type,expected_result",
    [
        ("facebook_ads|ad_id", TaxonTypeEnum.dimension, TelType(TelDataType.STRING, is_constant=False)),
        ("facebook_ads|date", TaxonTypeEnum.dimension, TelType(TelDataType.DATETIME, is_constant=False)),
        ("facebook_ads|done", TaxonTypeEnum.dimension, TelType(TelDataType.BOOLEAN, is_constant=False)),
        ("impressions", TaxonTypeEnum.metric, TelType(TelDataType.INTEGER, is_constant=False)),
        ("impressions_numeric", TaxonTypeEnum.metric, TelType(TelDataType.NUMERIC, is_constant=False)),
        ("fb_ad_id", TaxonTypeEnum.dimension, TelType(TelDataType.STRING, is_constant=False)),
        ("fb_tw_merged_ad_id", TaxonTypeEnum.dimension, TelType(TelDataType.STRING, is_constant=False)),
    ],
    ids=generate_test_id,
)
def test_taxon_expressions(tel_expression, taxon_type, expected_result):
    execute_test(tel_expression, taxon_type, expected_result)


@pytest.mark.parametrize(
    "tel_expression,expected_result",
    [
        ("lower('str')", TelType(TelDataType.STRING, is_constant=False)),
        ("lower(ad_id)", TelType(TelDataType.STRING, is_constant=False)),
        ("lower(fb_ad_id)", TelType(TelDataType.STRING, is_constant=False)),
        ("lower(fb_tw_merged_ad_id)", TelType(TelDataType.STRING, is_constant=False)),
        ("lower(42)", ExceptionResult('Argument 1 in function lower must be of type: string')),
        ("lower(date)", ExceptionResult('Argument 1 in function lower must be of type: string')),
        ("upper(ad_id)", TelType(TelDataType.STRING, is_constant=False)),
        ("upper(date)", ExceptionResult('Argument 1 in function upper must be of type: string')),
        ("trim(ad_id)", TelType(TelDataType.STRING, is_constant=False)),
        ("trim(date)", ExceptionResult('Argument 1 in function trim must be of type: string')),
    ],
    ids=generate_test_id,
)
def test_single_string_dimension_functions(tel_expression, expected_result):
    execute_test(tel_expression, TaxonTypeEnum.dimension, expected_result)


@pytest.mark.parametrize(
    "tel_expression,expected_result",
    [
        ("concat(42)", TelType(TelDataType.STRING, is_constant=False)),
        ("concat('str')", TelType(TelDataType.STRING, is_constant=False)),
        ("concat('str', 42)", TelType(TelDataType.STRING, is_constant=False)),
        ("concat(facebook_ads|ad_id)", TelType(TelDataType.STRING, is_constant=False)),
        ("concat(facebook_ads|date)", TelType(TelDataType.STRING, is_constant=False)),
        ("concat(facebook_ads|ad_id, 'str', 42)", TelType(TelDataType.STRING, is_constant=False)),
        ("concat(facebook_ads|ad_id, facebook_ads|ad_id)", TelType(TelDataType.STRING, is_constant=False)),
        ("concat(facebook_ads|date, facebook_ads|date)", TelType(TelDataType.STRING, is_constant=False)),
        ("concat(facebook_ads|ad_id, facebook_ads|date)", TelType(TelDataType.STRING, is_constant=False)),
        ("concat(fb_ad_id, fb_ad_id)", TelType(TelDataType.STRING, is_constant=False)),
        ("concat(fb_tw_merged_ad_id, fb_tw_merged_ad_id)", TelType(TelDataType.STRING, is_constant=False)),
    ],
    ids=generate_test_id,
)
def test_concat_function(tel_expression, expected_result):
    execute_test(tel_expression, TaxonTypeEnum.dimension, expected_result)


@pytest.mark.parametrize(
    "tel_expression,expected_result",
    [
        ("merge(facebook_ads|ad_id)", TelType(TelDataType.STRING, is_constant=False)),
        ("merge(facebook_ads|date)", TelType(TelDataType.DATETIME, is_constant=False)),
        ("merge(facebook_ads|ad_id, twitter|ad_id)", TelType(TelDataType.STRING, is_constant=False)),
        ("merge(facebook_ads|date, twitter|date)", TelType(TelDataType.DATETIME, is_constant=False)),
        ("merge(fb_ad_id)", TelType(TelDataType.STRING, is_constant=False)),
        ("merge(fb_ad_id, tw_ad_id)", TelType(TelDataType.STRING, is_constant=False)),
        (
            "merge(facebook_ads|ad_id, twitter|date)",
            ExceptionResult('Arguments in function merge must have compatible data types'),
        ),
    ],
    ids=generate_test_id,
)
def test_merge_function(tel_expression, expected_result):
    execute_test(tel_expression, TaxonTypeEnum.dimension, expected_result)


@pytest.mark.parametrize(
    "tel_expression,expected_result",
    [
        ("coalesce(42)", TelType(TelDataType.INTEGER, is_constant=True)),
        ("coalesce('str')", TelType(TelDataType.STRING, is_constant=True)),
        ("coalesce(42, 4.2)", TelType(TelDataType.NUMERIC, is_constant=False)),
        ("coalesce(facebook_ads|ad_id)", TelType(TelDataType.STRING, is_constant=False)),
        ("coalesce(facebook_ads|date)", TelType(TelDataType.DATETIME, is_constant=False)),
        ("coalesce(facebook_ads|ad_id, 'str')", TelType(TelDataType.STRING, is_constant=False)),
        ("coalesce(facebook_ads|ad_id, facebook_ads|ad_id)", TelType(TelDataType.STRING, is_constant=False)),
        ("coalesce(impressions, impressions_numeric)", TelType(TelDataType.NUMERIC, is_constant=False)),
        ("coalesce(fb_ad_id)", TelType(TelDataType.STRING, is_constant=False)),
        ("coalesce(fb_tw_merged_ad_id)", TelType(TelDataType.STRING, is_constant=False)),
        ("coalesce(42, 'str')", ExceptionResult('Arguments in function coalesce must have compatible data types')),
        (
            "coalesce(facebook_ads|ad_id, 42)",
            ExceptionResult('Arguments in function coalesce must have compatible data types'),
        ),
        (
            "coalesce(facebook_ads|ad_id, facebook_ads|date)",
            ExceptionResult('Arguments in function coalesce must have compatible data types'),
        ),
    ],
    ids=generate_test_id,
)
def test_coalesce_function(tel_expression, expected_result):
    execute_test(tel_expression, TaxonTypeEnum.metric, expected_result)


@pytest.mark.parametrize(
    "tel_expression,expected_result",
    [
        ("contains('str', 'str')", TelType(TelDataType.BOOLEAN, is_constant=False)),
        ("contains('str', 'str', 'str2')", TelType(TelDataType.BOOLEAN, is_constant=False)),
        ("contains(facebook_ads|ad_id, 'str')", TelType(TelDataType.BOOLEAN, is_constant=False)),
        ("contains(facebook_ads|ad_id, 'str', 'str2')", TelType(TelDataType.BOOLEAN, is_constant=False)),
        ("contains(fb_ad_id, 'str')", TelType(TelDataType.BOOLEAN, is_constant=False)),
        ("contains(fb_tw_merged_ad_id, 'str')", TelType(TelDataType.BOOLEAN, is_constant=False)),
        ("contains(42, 'str')", ExceptionResult('Argument 1 in function contains must be of type: string')),
        ("contains('str', 42)", ExceptionResult('Argument 2 in function contains must be of type: string')),
        (
            "contains(facebook_ads|date, 'str')",
            ExceptionResult('Argument 1 in function contains must be of type: string'),
        ),
        (
            "contains(facebook_ads|ad_id, 'str', 42)",
            ExceptionResult('Argument 3 in function contains must be of type: string'),
        ),
    ],
    ids=generate_test_id,
)
def test_contains_function(tel_expression, expected_result):
    execute_test(tel_expression, TaxonTypeEnum.dimension, expected_result)


@pytest.mark.parametrize(
    "tel_expression,expected_result",
    [
        ("iff(true, 42)", TelType(TelDataType.INTEGER, is_constant=False)),
        ("iff(true, 42, 42)", TelType(TelDataType.INTEGER, is_constant=False)),
        ("iff(true, 42, 4.2)", TelType(TelDataType.NUMERIC, is_constant=False)),
        ("iff(true, impressions)", TelType(TelDataType.INTEGER, is_constant=False)),
        ("iff(true, impressions, 42)", TelType(TelDataType.INTEGER, is_constant=False)),
        ("iff(true, impressions, 4.2)", TelType(TelDataType.NUMERIC, is_constant=False)),
        ("iff(true, impressions, impressions)", TelType(TelDataType.INTEGER, is_constant=False)),
        ("iff(true, impressions, impressions_numeric)", TelType(TelDataType.NUMERIC, is_constant=False)),
        ("iff('str', 42)", ExceptionResult('Argument 1 in function iff must be of type: boolean')),
        ("iff(true, 42, 'str')", ExceptionResult('Outcome arguments in function iff must have compatible data types')),
    ],
    ids=generate_test_id,
)
def test_iff_function(tel_expression, expected_result):
    execute_test(tel_expression, TaxonTypeEnum.metric, expected_result)


@pytest.mark.parametrize(
    "tel_expression,expected_result",
    [
        ("ifs(true, 42)", TelType(TelDataType.INTEGER, is_constant=False)),
        ("ifs(true, 42, true, 42, 42)", TelType(TelDataType.INTEGER, is_constant=False)),
        ("ifs(true, 42, true, 42, 4.2)", TelType(TelDataType.NUMERIC, is_constant=False)),
        ("ifs(true, impressions)", TelType(TelDataType.INTEGER, is_constant=False)),
        ("ifs(true, impressions, true, 42, 42)", TelType(TelDataType.INTEGER, is_constant=False)),
        ("ifs(true, impressions, true, 42, 4.2)", TelType(TelDataType.NUMERIC, is_constant=False)),
        ("ifs(true, impressions, true, impressions)", TelType(TelDataType.INTEGER, is_constant=False)),
        ("ifs(true, impressions, true, impressions_numeric)", TelType(TelDataType.NUMERIC, is_constant=False)),
        ("ifs('str', 42)", ExceptionResult('Argument 1 in function ifs must be of type: boolean')),
        ("ifs(true, 42, 'str', 42)", ExceptionResult('Argument 3 in function ifs must be of type: boolean')),
        ("ifs(true, 42, 'str')", ExceptionResult('Outcome arguments in function ifs must have compatible data types')),
        (
            "ifs(true, 42, true, 'str')",
            ExceptionResult('Outcome arguments in function ifs must have compatible data types'),
        ),
        (
            "ifs(true, 42, true, 42, 'str')",
            ExceptionResult('Outcome arguments in function ifs must have compatible data types'),
        ),
    ],
    ids=generate_test_id,
)
def test_ifs_function(tel_expression, expected_result):
    execute_test(tel_expression, TaxonTypeEnum.metric, expected_result)


@pytest.mark.parametrize(
    "tel_expression,expected_result",
    [
        ("parse('str|str2', '|', 1)", TelType(TelDataType.ANY, is_constant=False)),
        ("parse(facebook_ads|ad_id, '|', 1)", TelType(TelDataType.ANY, is_constant=False)),
        ("parse(fb_ad_id, '|', 1)", TelType(TelDataType.ANY, is_constant=False)),
        ("parse(fb_tw_merged_ad_id, '|', 1)", TelType(TelDataType.ANY, is_constant=False)),
        ("parse(42, '|', 1)", ExceptionResult('Argument 1 in function parse must be of type: string')),
        ("parse(facebook_ads|date, '|', 1)", ExceptionResult('Argument 1 in function parse must be of type: string')),
        ("parse(facebook_ads|ad_id, 42, 1)", ExceptionResult('Argument 2 in function parse must be of type: string')),
        (
            "parse(facebook_ads|ad_id, '|', 'str')",
            ExceptionResult('Argument 3 in function parse must be of type: integer'),
        ),
    ],
    ids=generate_test_id,
)
def test_parse_function(tel_expression, expected_result):
    execute_test(tel_expression, TaxonTypeEnum.dimension, expected_result)


@pytest.mark.parametrize(
    "tel_expression,taxon_type,expected_result",
    [
        # TODO: comment out this section (or delete it) once we agree on how we want to handle "parse" function
        # (
        #     "parse(facebook_ads|ad_id, '|', 1) + impressions",
        #     TaxonTypeEnum.metric,
        #     TelType(TelDataType.INTEGER, is_constant=False),
        # ),
        # (
        #     "parse(facebook_ads|ad_id, '|', 1) + impressions - 4.2",
        #     TaxonTypeEnum.metric,
        #     TelType(TelDataType.NUMERIC, is_constant=False),
        # ),
        (
            "merge(parse(facebook_ads|ad_id, '|', 1), twitter|ad_id)",
            TaxonTypeEnum.dimension,
            TelType(TelDataType.STRING, is_constant=False),
        ),
    ],
    ids=generate_test_id,
)
def test_parse_function_complex(tel_expression, taxon_type, expected_result):
    execute_test(tel_expression, taxon_type, expected_result)


@pytest.mark.parametrize(
    "tel_expression,expected_result",
    [
        ("convert_timezone(facebook_ads|date, 'UTC')", TelType(TelDataType.DATETIME, is_constant=False)),
        ("convert_timezone(facebook_ads|date, 'UTC', 'CET')", TelType(TelDataType.DATETIME, is_constant=False)),
        (
            "convert_timezone(facebook_ads|ad_id, 'UTC', 'CET')",
            ExceptionResult('Argument 1 in function convert_timezone must be of type: datetime'),
        ),
        (
            "convert_timezone(facebook_ads|date, 42)",
            ExceptionResult('Argument 2 in function convert_timezone is not a valid timezone name'),
        ),
        (
            "convert_timezone(facebook_ads|date, facebook_ads|ad_id)",
            ExceptionResult('Argument 2 in function convert_timezone is not a valid timezone name'),
        ),
        (
            "convert_timezone(facebook_ads|date, 'UTC', 42)",
            ExceptionResult('Argument 3 in function convert_timezone is not a valid timezone name'),
        ),
        (
            "convert_timezone(facebook_ads|date, 'UTC', facebook_ads|ad_id)",
            ExceptionResult('Argument 3 in function convert_timezone is not a valid timezone name'),
        ),
    ],
    ids=generate_test_id,
)
def test_convert_timezone_function(tel_expression, expected_result):
    execute_test(tel_expression, TaxonTypeEnum.dimension, expected_result)


@pytest.mark.parametrize(
    "tel_expression,expected_result",
    [
        ("date_diff('SECOND', twitter|date, twitter|date)", TelType(TelDataType.DATETIME, is_constant=False)),
        ("date_diff('MINUTE', twitter|date, twitter|date)", TelType(TelDataType.DATETIME, is_constant=False)),
        ("date_diff('HOUR', twitter|date, twitter|date)", TelType(TelDataType.DATETIME, is_constant=False)),
        ("date_diff('DAY', twitter|date, twitter|date)", TelType(TelDataType.DATETIME, is_constant=False)),
        ("date_diff('WEEK', twitter|date, twitter|date)", TelType(TelDataType.DATETIME, is_constant=False)),
        ("date_diff('MONTH', twitter|date, twitter|date)", TelType(TelDataType.DATETIME, is_constant=False)),
        ("date_diff('YEAR', twitter|date, twitter|date)", TelType(TelDataType.DATETIME, is_constant=False)),
        (
            "date_diff(42, twitter|date, twitter|date)",
            ExceptionResult('Argument 1 in function date_diff must be of type: string'),
        ),
        (
            "date_diff('SECOND', twitter|ad_id, twitter|date)",
            ExceptionResult('Argument 2 in function date_diff must be of type: datetime'),
        ),
        (
            "date_diff('SECOND', twitter|date, twitter|ad_id)",
            ExceptionResult('Argument 3 in function date_diff must be of type: datetime'),
        ),
    ],
    ids=generate_test_id,
)
def test_date_diff_function(tel_expression, expected_result):
    execute_test(tel_expression, TaxonTypeEnum.dimension, expected_result)


@pytest.mark.parametrize(
    "tel_expression,expected_result",
    [
        ("date_hour(facebook_ads|date)", TelType(TelDataType.DATETIME, is_constant=False)),
        ("date(facebook_ads|date)", TelType(TelDataType.DATETIME, is_constant=False)),
        ("date_week(facebook_ads|date)", TelType(TelDataType.DATETIME, is_constant=False)),
        ("date_month(facebook_ads|date)", TelType(TelDataType.DATETIME, is_constant=False)),
        ("hour_of_day(facebook_ads|date)", TelType(TelDataType.INTEGER, is_constant=False)),
        ("day_of_week(facebook_ads|date)", TelType(TelDataType.INTEGER, is_constant=False)),
        ("week_of_year(facebook_ads|date)", TelType(TelDataType.INTEGER, is_constant=False)),
        ("month_of_year(facebook_ads|date)", TelType(TelDataType.INTEGER, is_constant=False)),
        ("year(facebook_ads|date)", TelType(TelDataType.INTEGER, is_constant=False)),
        ("year('str')", ExceptionResult('Argument 1 in function year must be of type: datetime')),
        ("year(facebook_ads|ad_id)", ExceptionResult('Argument 1 in function year must be of type: datetime')),
    ],
    ids=generate_test_id,
)
def test_single_datetime_dimension_functions(tel_expression, expected_result):
    execute_test(tel_expression, TaxonTypeEnum.dimension, expected_result)


@pytest.mark.parametrize(
    "tel_expression,expected_result",
    [
        ("1 + 2", TelType(TelDataType.INTEGER, is_constant=True)),
        ("1 + 2.0", TelType(TelDataType.NUMERIC, is_constant=True)),
        ("impressions + 1", TelType(TelDataType.INTEGER, is_constant=False)),
        ("impressions + 1.0", TelType(TelDataType.NUMERIC, is_constant=False)),
        ("impressions + impressions", TelType(TelDataType.INTEGER, is_constant=False)),
        ("impressions + impressions_numeric", TelType(TelDataType.NUMERIC, is_constant=False)),
        ("1 + 2 - 3", TelType(TelDataType.INTEGER, is_constant=True)),
        ("1 + 2.0 - 3", TelType(TelDataType.NUMERIC, is_constant=True)),
        ("impressions + 1 - impressions", TelType(TelDataType.INTEGER, is_constant=False)),
        ("impressions + 1.0 - impressions", TelType(TelDataType.NUMERIC, is_constant=False)),
        ("impressions + impressions_numeric - impressions", TelType(TelDataType.NUMERIC, is_constant=False)),
        ("'str' + 1", ExceptionResult('Operand 1 in addition expression must be of type: number')),
        ("1 - 'str'", ExceptionResult('Operand 2 in subtraction expression must be of type: number')),
        # raw_data is metric with data type text
        ("raw_data + impressions", ExceptionResult('Operand 1 in addition expression must be of type: number')),
        ("impressions - raw_data", ExceptionResult('Operand 2 in subtraction expression must be of type: number')),
    ],
    ids=generate_test_id,
)
def test_additive_expression(tel_expression, expected_result):
    execute_test(tel_expression, TaxonTypeEnum.metric, expected_result)


@pytest.mark.parametrize(
    "tel_expression,expected_result",
    [
        ("1 * 2", TelType(TelDataType.INTEGER, is_constant=True)),
        ("1 * 2.0", TelType(TelDataType.NUMERIC, is_constant=True)),
        ("impressions * 1", TelType(TelDataType.INTEGER, is_constant=False)),
        ("impressions * 1.0", TelType(TelDataType.NUMERIC, is_constant=False)),
        ("impressions * impressions", TelType(TelDataType.INTEGER, is_constant=False)),
        ("impressions * impressions_numeric", TelType(TelDataType.NUMERIC, is_constant=False)),
        ("1 * 2 / 3", TelType(TelDataType.NUMERIC, is_constant=True)),
        ("1 * 2.0 / 3", TelType(TelDataType.NUMERIC, is_constant=True)),
        ("impressions * 1 / impressions", TelType(TelDataType.INTEGER, is_constant=False)),
        ("impressions * 1.0 / impressions", TelType(TelDataType.NUMERIC, is_constant=False)),
        ("impressions * impressions_numeric / impressions", TelType(TelDataType.NUMERIC, is_constant=False)),
        ("'str' * 1", ExceptionResult('Operand 1 in multiplication expression must be of type: number')),
        ("1 / 'str'", ExceptionResult('Operand 2 in division expression must be of type: number')),
        # raw_data is metric with data type text
        ("raw_data * impressions", ExceptionResult('Operand 1 in multiplication expression must be of type: number')),
        ("impressions / raw_data", ExceptionResult('Operand 2 in division expression must be of type: number')),
    ],
    ids=generate_test_id,
)
def test_multiplication_expression(tel_expression, expected_result):
    execute_test(tel_expression, TaxonTypeEnum.metric, expected_result)


@pytest.mark.parametrize(
    "tel_expression,expected_result",
    [
        ("1 == 2", TelType(TelDataType.BOOLEAN, is_constant=True)),
        ("1 == 2.0", TelType(TelDataType.BOOLEAN, is_constant=True)),
        ("1 == impressions", TelType(TelDataType.BOOLEAN, is_constant=False)),
        ("1 == impressions_numeric", TelType(TelDataType.BOOLEAN, is_constant=False)),
        ("impressions == impressions", TelType(TelDataType.BOOLEAN, is_constant=False)),
        ("impressions == impressions_numeric", TelType(TelDataType.BOOLEAN, is_constant=False)),
        ("impressions != impressions_numeric", TelType(TelDataType.BOOLEAN, is_constant=False)),
        ("impressions > impressions_numeric", TelType(TelDataType.BOOLEAN, is_constant=False)),
        ("impressions < impressions_numeric", TelType(TelDataType.BOOLEAN, is_constant=False)),
        ("impressions >= impressions_numeric", TelType(TelDataType.BOOLEAN, is_constant=False)),
        ("impressions <= impressions_numeric", TelType(TelDataType.BOOLEAN, is_constant=False)),
        ("true || false", TelType(TelDataType.BOOLEAN, is_constant=True)),
        ("true && false", TelType(TelDataType.BOOLEAN, is_constant=True)),
        ("not true", TelType(TelDataType.BOOLEAN, is_constant=True)),
        ("not (true || false)", TelType(TelDataType.BOOLEAN, is_constant=True)),
        ("not (impressions == impressions)", TelType(TelDataType.BOOLEAN, is_constant=False)),
        ("1 is null", TelType(TelDataType.BOOLEAN, is_constant=True)),
        ("impressions is null", TelType(TelDataType.BOOLEAN, is_constant=False)),
        ("impressions is not null", TelType(TelDataType.BOOLEAN, is_constant=False)),
        ("1 == 'str'", ExceptionResult('Operands in logical expression must have compatible data types')),
        ("1 == facebook_ads|ad_id", ExceptionResult('Operands in logical expression must have compatible data types')),
        (
            "impressions != facebook_ads|ad_id",
            ExceptionResult('Operands in logical expression must have compatible data types'),
        ),
        ("not 1", ExceptionResult('Operand in not expression must be of type: boolean')),
        ("not generic_spend", ExceptionResult('Operand in not expression must be of type: boolean')),
        ("not fb_tw_merged_ad_id", ExceptionResult('Operand in not expression must be of type: boolean')),
    ],
    ids=generate_test_id,
)
def test_logical_expression(tel_expression, expected_result):
    execute_test(tel_expression, TaxonTypeEnum.metric, expected_result)


@pytest.mark.parametrize(
    "tel_expression,expected_result",
    [
        ("override(fb_tw_merged_ad_id, 'override-mapping-slug')", TelType(TelDataType.STRING, is_constant=False)),
        ("override(fb_tw_merged_ad_id, 'override-mapping-slug')", TelType(TelDataType.STRING, is_constant=False)),
        ("override('test-string', 'override-mapping-slug')", TelType(TelDataType.STRING, is_constant=False)),
    ],
    ids=generate_test_id,
)
def test_override_expression(tel_expression, expected_result):
    execute_test(tel_expression, TaxonTypeEnum.dimension, expected_result)


@pytest.mark.parametrize(
    "tel_expression,expected_result",
    [
        ("overall(impressions)", TelType(TelDataType.INTEGER, is_constant=False)),
        ("overall(impressions_numeric)", TelType(TelDataType.NUMERIC, is_constant=False)),
        ("cumulative(impressions, date)", TelType(TelDataType.INTEGER, is_constant=False)),
        ("cumulative(impressions_numeric, date)", TelType(TelDataType.NUMERIC, is_constant=False)),
    ],
    ids=generate_test_id,
)
def test_window_functions(tel_expression, expected_result):
    execute_test(tel_expression, TaxonTypeEnum.metric, expected_result)
