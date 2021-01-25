import pytest

from panoramic.cli.husky.core.taxonomy.aggregations import AggregationDefinition
from panoramic.cli.husky.core.taxonomy.enums import AggregationType, TaxonTypeEnum
from panoramic.cli.husky.core.tel.exceptions import TelExpressionException
from panoramic.cli.husky.core.tel.result import (
    ExprResult,
    PostFormula,
    PreFormula,
    TelPhase,
)
from panoramic.cli.husky.core.tel.tel_dialect import TaxonTelDialect
from panoramic.cli.husky.service.context import SNOWFLAKE_HUSKY_CONTEXT
from tests.panoramic.cli.husky.test.mocks.core.taxonomy import TAXON_MAP
from tests.panoramic.cli.husky.test.tel_utils import (
    ExprException,
    assert_result_formulas,
    get_test_case_id,
)


def execute_test(test_input, expected):
    taxon_type = TaxonTypeEnum.metric
    if len(test_input) == 3:
        taxon_type = test_input[2]
    if isinstance(expected, ExprException):
        with pytest.raises(TelExpressionException) as exc_info:
            TaxonTelDialect().render(
                test_input[0],
                SNOWFLAKE_HUSKY_CONTEXT,
                data_sources=test_input[1],
                taxon_map=TAXON_MAP,
                taxon_type=taxon_type,
            )
        assert exc_info.value.args[0] == expected.msg
    else:
        expected.phase = TelPhase.metric_post
        res = TaxonTelDialect().render(
            test_input[0],
            SNOWFLAKE_HUSKY_CONTEXT,
            data_sources=test_input[1],
            taxon_map=TAXON_MAP,
            taxon_type=taxon_type,
        )
        assert_result_formulas(res, expected)


@pytest.mark.parametrize(
    "test_input,expected",
    [
        # error cases
        (
            ("spend + 40 + (avg_spend * 10)", set(), TaxonTypeEnum.metric),
            ExprException(
                'It was not possible to deduce aggregation type. '
                'Occurred at position 1, line 1 in expression "spend + 40 + (avg_spend * 10)"'
            ),
        ),
        (
            ("enhanced_cpm_no_agg + avg_spend", set(), TaxonTypeEnum.metric),
            ExprException(
                'It was not possible to deduce aggregation type. '
                'Occurred at position 1, line 1 in expression "enhanced_cpm_no_agg + avg_spend"'
            ),
        ),
        # Valid  cases
        (
            ("123 + 567", set(), TaxonTypeEnum.metric),
            ExprResult(
                data_source_formula_templates=[],
                dimension_formulas=[],
                pre_formulas=[],
                post_formula=PostFormula('690'),
            ),
        ),
        (
            ("123 + (twitter|spend / 567)", {'twitter'}, TaxonTypeEnum.metric),
            ExprResult(
                data_source_formula_templates=[],
                dimension_formulas=[],
                pre_formulas=[
                    PreFormula(
                        '(twitter_spend_68657fbb141b10c8 / nullif(567, 0))',
                        '__1',
                        AggregationDefinition(type=AggregationType.sum),
                        None,
                    )
                ],
                post_formula=PostFormula('coalesce(123, 0) + coalesce(__1, 0)'),
            ),
        ),
        (
            ("444.3 / (computed_metric_avg - 1)", set(), TaxonTypeEnum.metric),
            ExprResult(
                data_source_formula_templates=[],
                dimension_formulas=[],
                pre_formulas=[
                    PreFormula('1000 * avg_spend', '__1', AggregationDefinition(type=AggregationType.avg), None)
                ],
                post_formula=PostFormula('444.3 / nullif((coalesce(__1, 0) - coalesce(1, 0)), 0)'),
            ),
        ),
        (
            ("spend + cpm_no_agg", set(), TaxonTypeEnum.metric),
            ExprResult(
                data_source_formula_templates=[],
                dimension_formulas=[],
                pre_formulas=[
                    PreFormula('''spend''', '''__1''', AggregationDefinition(type=AggregationType.sum), None),
                    PreFormula('''1000 * spend''', '''__2''', AggregationDefinition(type=AggregationType.sum), None),
                    PreFormula('''impressions''', '''__3''', AggregationDefinition(type=AggregationType.sum), None),
                ],
                post_formula=PostFormula('coalesce(__1, 0) + coalesce(__2 / nullif(__3, 0), 0)'),
            ),
        ),
        (
            ("avg_spend / computed_metric_avg", set(), TaxonTypeEnum.metric),
            ExprResult(
                data_source_formula_templates=[],
                dimension_formulas=[],
                pre_formulas=[
                    PreFormula('''avg_spend''', '''__1''', AggregationDefinition(type=AggregationType.avg), None),
                    PreFormula(
                        '''1000 * avg_spend''', '''__2''', AggregationDefinition(type=AggregationType.avg), None
                    ),
                ],
                post_formula=PostFormula('__1 / nullif(__2, 0)'),
            ),
        ),
        (
            ("(ad_id != 'test')", set(), TaxonTypeEnum.dimension),
            ExprResult(
                data_source_formula_templates=[],
                dimension_formulas=[
                    PreFormula(
                        "(ad_id != 'test')",
                        '''__1''',
                        AggregationDefinition(type=AggregationType.not_set, params=None),
                        None,
                    )
                ],
                pre_formulas=[
                    PreFormula(
                        '''__1''', '''__2''', AggregationDefinition(type=AggregationType.group_by, params=None), None
                    )
                ],
                post_formula=PostFormula('__2'),
            ),
        ),
        (
            ("true || (ad_id != 'test')", set(), TaxonTypeEnum.dimension),
            ExprResult(
                data_source_formula_templates=[],
                dimension_formulas=[
                    PreFormula(
                        "(ad_id != 'test')",
                        '''__1''',
                        AggregationDefinition(type=AggregationType.not_set, params=None),
                        None,
                    )
                ],
                pre_formulas=[
                    PreFormula(
                        '''__1''', '''__2''', AggregationDefinition(type=AggregationType.group_by, params=None), None
                    )
                ],
                post_formula=PostFormula('true'),
            ),
        ),
        (
            ("true || not (ad_id > 'test')", set(), TaxonTypeEnum.dimension),
            ExprResult(
                data_source_formula_templates=[],
                dimension_formulas=[
                    PreFormula(
                        "(ad_id <= 'test')",
                        '''__1''',
                        AggregationDefinition(type=AggregationType.not_set, params=None),
                        None,
                    )
                ],
                pre_formulas=[
                    PreFormula(
                        '''__1''', '''__2''', AggregationDefinition(type=AggregationType.group_by, params=None), None
                    )
                ],
                post_formula=PostFormula('true'),
            ),
        ),
        (
            ("ad_id is null || not (ad_id <= 'test')", set(), TaxonTypeEnum.dimension),
            ExprResult(
                data_source_formula_templates=[],
                dimension_formulas=[
                    PreFormula(
                        '''ad_id IS NULL''',
                        '''__1''',
                        AggregationDefinition(type=AggregationType.not_set, params=None),
                        None,
                    ),
                    PreFormula(
                        '''(ad_id > 'test')''',
                        '''__3''',
                        AggregationDefinition(type=AggregationType.not_set, params=None),
                        None,
                    ),
                ],
                pre_formulas=[
                    PreFormula(
                        '''__1''', '''__2''', AggregationDefinition(type=AggregationType.group_by, params=None), None
                    ),
                    PreFormula(
                        '''__3''', '''__4''', AggregationDefinition(type=AggregationType.group_by, params=None), None
                    ),
                ],
                post_formula=PostFormula('__2 OR __4'),
            ),
        ),
        (
            ("ad_id is not null && not (ad_id <= '10')", set(), TaxonTypeEnum.dimension),
            ExprResult(
                data_source_formula_templates=[],
                dimension_formulas=[
                    PreFormula(
                        "ad_id IS NOT NULL",
                        '''__1''',
                        AggregationDefinition(type=AggregationType.not_set, params=None),
                        None,
                    ),
                    PreFormula(
                        '''(ad_id > '10')''',
                        '''__3''',
                        AggregationDefinition(type=AggregationType.not_set, params=None),
                        None,
                    ),
                ],
                pre_formulas=[
                    PreFormula(
                        '''__1''', '''__2''', AggregationDefinition(type=AggregationType.group_by, params=None), None
                    ),
                    PreFormula(
                        '''__3''', '''__4''', AggregationDefinition(type=AggregationType.group_by, params=None), None
                    ),
                ],
                post_formula=PostFormula('__2 AND __4'),
            ),
        ),
    ],
    ids=get_test_case_id,
)
def test_deduce_simple_aggregations(test_input, expected):
    execute_test(test_input, expected)
