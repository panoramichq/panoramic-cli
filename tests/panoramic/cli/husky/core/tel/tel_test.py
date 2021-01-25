import pytest

from panoramic.cli.husky.core.sql_alchemy_util import compile_query
from panoramic.cli.husky.core.taxonomy.enums import TaxonTypeEnum
from panoramic.cli.husky.core.tel.exceptions import TelExpressionException
from panoramic.cli.husky.core.tel.result import ExprResult, PostFormula
from panoramic.cli.husky.core.tel.tel import Tel
from panoramic.cli.husky.core.tel.tel_dialect import TaxonTelDialect
from panoramic.cli.husky.service.context import SNOWFLAKE_HUSKY_CONTEXT
from tests.panoramic.cli.husky.test.capture_asserter import BaseCaptureAsserter
from tests.panoramic.cli.husky.test.mocks.core.taxonomy import TAXON_MAP
from tests.panoramic.cli.husky.test.tel_utils import get_test_case_id


class ExprResultAsserter(BaseCaptureAsserter):
    def assert_item(self, actual, expected):
        if 'exception_class' in expected:
            assert actual == expected, 'Exceptions dont match.'
        else:
            assert (
                actual['data_source_formula_templates'] == expected['data_source_formula_templates']
            ), 'data_source_formulas dont match'
            assert actual['dimension_formulas'] == expected['dimension_formulas'], 'dimension_formulas dont match'
            assert actual['pre_formulas'] == expected['pre_formulas'], 'pre_formulas dont match'
            assert actual['post_formula'] == expected['post_formula'], "post_formula doesn't match"
            assert actual['phase'] == expected['phase'], 'phase dont match'
            assert actual['override_mappings'] == expected['override_mappings'], 'override mappings dont match'
            assert actual['invalid_value'] == expected['invalid_value'], "invalid_value doesn't match"

    def item_to_primitive(self, item):
        if isinstance(item, ExprResult):
            result_dict = dict()
            result_dict['data_source_formula_templates'] = item.data_source_formula_templates
            result_dict['dimension_formulas'] = item.dimension_formulas
            result_dict['pre_formulas'] = item.pre_formulas
            result_dict['post_formula'] = item.post_formula
            result_dict['phase'] = item.phase
            result_dict['override_mappings'] = sorted(list(item.override_mappings))
            result_dict['invalid_value'] = item.invalid_value
            primitive = {k: self.item_to_primitive(v) for k, v in result_dict.items()}
            return primitive
        elif isinstance(item, PostFormula):
            post_formula_dict = {'sql': compile_query(item._sql)}
            if item.template is not None:
                template = compile_query(item.template)
                if template != post_formula_dict['sql']:
                    post_formula_dict['template'] = template
            if len(item.exclude_slugs) > 0:
                post_formula_dict['exclude_slugs'] = sorted([self.item_to_primitive(i) for i in item.exclude_slugs])
            return post_formula_dict
        elif isinstance(item, TelExpressionException):
            return {'exception_class': str(item.__class__), 'api_response_message': str(item)}
        else:
            return super().item_to_primitive(item)


def execute_test(request, test_input):
    taxon_type = TaxonTypeEnum.metric
    taxon_slug = ''
    if len(test_input) == 3:
        taxon_type = test_input[2]
    if len(test_input) == 4:
        taxon_slug = test_input[3]
    asserter = ExprResultAsserter(request, test_description_generator=get_test_case_id)
    try:
        res = TaxonTelDialect().render(
            test_input[0],
            SNOWFLAKE_HUSKY_CONTEXT,
            data_sources=test_input[1],
            taxon_map=TAXON_MAP,
            taxon_type=taxon_type,
            taxon_slug=taxon_slug,
        )
        asserter.eval(test_input, res)
    except TelExpressionException as e:
        asserter.eval(test_input, e)


@pytest.mark.parametrize(
    "test_input",
    [
        '(spend + 10/2) / views ',
        '((spend + impressions) / 2) * cpm / cpm',
        '(spend / impressions) * 30',
        'spend ',
        '30 + spend ',
        'spend + impressions + 30 ',
        '30 ',
        'spend_w_impr ',
        "spend / impressions",
        "pinterest|total_conversions_value",
    ],
)
def test_deep_advanced_render(request, test_input):
    res = TaxonTelDialect().render(test_input, SNOWFLAKE_HUSKY_CONTEXT, taxon_map=TAXON_MAP)
    ExprResultAsserter(request, test_description_generator=str).eval(test_input, res)


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (("?facebook|spend + adwords|spend", {'adwords'}), {'adwords|spend'}),
        (("facebook|spend + adwords|spend", None), {'facebook|spend', 'adwords|spend'}),
        (("1+1", None), set()),
        (("spend + impressions", None), {'spend', 'impressions'}),
        (("spend + impressions * cpm / cpv", None), {'spend', 'impressions', 'cpm', 'cpv'}),
        (
            ("(comments + reactions + shares + video_views_to_10s) / impressions", None),
            {'comments', 'reactions', 'shares', 'video_views_to_10s', 'impressions'},
        ),
        (("(purchase_value + purchase_value_app) / spend", None), {'purchase_value', 'purchase_value_app', 'spend'}),
    ],
)
def test_used_taxons(test_input, expected):
    assert Tel.get_used_taxon_slugs_shallow(test_input[0], test_input[1]).all_slugs == expected


@pytest.mark.parametrize(
    "test_input", ["enhanced_cpm", "spend / impressions", "spend", '"str_constant"', "facebook_ads|spend"]
)
def test_transpile_to_comparison_snowflake_sql(request, test_input):
    res = TaxonTelDialect().render(test_input, SNOWFLAKE_HUSKY_CONTEXT, comparison=True, taxon_map=TAXON_MAP)
    ExprResultAsserter(request, test_description_generator=str).eval(test_input, res)


@pytest.mark.parametrize("test_input", ["spend / + impressions", "taxon_not_existant_asdasd + impressions"])
def test_errors(request, test_input):
    with pytest.raises(TelExpressionException) as exc_info:
        TaxonTelDialect().render(test_input, SNOWFLAKE_HUSKY_CONTEXT, taxon_map=TAXON_MAP)
    ExprResultAsserter(request, test_description_generator=str).eval(test_input, exc_info.value)


@pytest.mark.parametrize(
    "test_input",
    [
        ('(?facebook_ads|spend + ?twitter|spend) / adwords|impressions', {'adwords'}),
        ('?facebook_ads|spend + ?twitter|spend + adwords|spend', {'adwords'}),
        ('(facebook_ads|spend + ?twitter|spend) / (facebook_ads|impressions + ?twitter|impressions)', {'facebook_ads'}),
        ('enhanced_cpm_2', {'facebook_ads'}),
        ('generic_cpm', {'facebook_ads'}),
        ('generic_cpm', {'facebook_ads', 'adwords'}),
        ('generic_spend2', {'facebook_ads'}),
        ('generic_spend2', {'facebook_ads', 'adwords'}),
    ],
)
def test_namespace_taxons_render(request, test_input):
    execute_test(request, test_input)


@pytest.mark.parametrize(
    "test_input",
    [
        ('coalesce()', {'xxx'}),
        ('coalesce(?facebook_ads|spend, 1)', {'xxx'}),
        ('coalesce(facebook_ads|spend)', {'facebook_ads'}),
        # Test with division of coalesce with other division
        ('coalesce(generic_cpm,enhanced_cpm)', {'twitter', 'facebook_ads'}),
        ('coalesce(?facebook_ads|spend, twitter|spend)', {'twitter'}),
        # Test with complex cpm at the end.
        ('coalesce(facebook_ads|spend,twitter|spend,enhanced_cpm)', {'facebook_ads', 'twitter'}),
        # Test with division of coalesce with other division
        (
            'coalesce(twitter|spend / twitter|impressions) / coalesce(facebook_ads|spend / facebook_ads|impressions)',
            {'twitter', 'facebook_ads'},
        ),
    ],
)
def test_coalesce_render(request, test_input):
    execute_test(request, test_input)


@pytest.mark.parametrize(
    "test_input",
    [
        # Different dimension
        ('iff(?twitter|spend<10,100)', {'asdasd'}),
        ('iff(generic_spend+generic_impressions<10,1,2)', {'facebook_ads'}),
        ('iff(generic_spend>generic_impressions, generic_spend, generic_impressions)', {'facebook_ads', 'twitter'}),
        ('iff(facebook_ads|spend > twitter|spend,facebook_ads|spend,twitter|spend)', {'facebook_ads', 'twitter'}),
        ('iff(generic_cpm<10,10,facebook_ads|spend)', {'facebook_ads'}),
        # Nested iffs
        ('iff(twitter|spend<10,iff(twitter|spend<5,4,9),11)', {'twitter'}),
        # Different phases
        ("iff(twitter|ad_id == 'id', twitter|ad_id)", {'twitter'}, TaxonTypeEnum.dimension),
        (
            "iff(fb_tw_merged_ad_id == 'id', twitter|ad_id, facebook_ads|ad_id)",
            {'twitter', 'facebook_ads'},
            TaxonTypeEnum.dimension,
        ),
        ("iff(fb_tw_merged_ad_id == 'id', twitter|spend)", {'twitter', 'facebook_ads'}, TaxonTypeEnum.metric),
        ("iff(generic_spend > 0, generic_spend, twitter|spend)", {'twitter', 'facebook_ads'}, TaxonTypeEnum.metric),
        ("iff(generic_cpm > 0, generic_cpm, generic_spend)", {'twitter', 'facebook_ads'}, TaxonTypeEnum.metric),
        # Different invalid values
        ("iff(?unknown|spend == 0, twitter|spend)", {'twitter'}, TaxonTypeEnum.metric),
        ("iff(twitter|spend == 0, ?unknown|spend)", {'twitter'}, TaxonTypeEnum.metric),
        ("iff(twitter|spend == 0, twitter|spend, ?unknown|spend)", {'twitter'}, TaxonTypeEnum.metric),
        ("iff(fb_tw_merged_ad_id == 'id', ?unknown|spend, twitter|spend)", {'twitter'}, TaxonTypeEnum.metric),
        ("iff(fb_tw_merged_ad_id == 'id', ?twitter|unknown, twitter|ad_id)", {'twitter'}, TaxonTypeEnum.dimension),
        ("iff(fb_tw_merged_ad_id == 'id', fb_tw_merged_ad_id, ?unknown)", {'twitter'}, TaxonTypeEnum.dimension),
        (
            "iff(fb_tw_merged_ad_id == 'tw', ?twitter|spend, iff(fb_tw_merged_ad_id == 'fb', ?facebook_ads|spend, ?unknown|spend))",
            {'twitter'},
            TaxonTypeEnum.metric,
        ),
        (
            "iff(fb_tw_merged_ad_id == 'tw', ?twitter|spend, iff(fb_tw_merged_ad_id == 'fb', ?facebook_ads|spend, ?unknown|spend))",
            {'twitter', 'facebook_ads'},
            TaxonTypeEnum.metric,
        ),
        # Constants as metrics
        (
            "iff(fb_tw_merged_ad_id == 'tw', 10, iff(fb_tw_merged_ad_id == 'fb', 20, iff(?unknown == 'un', 30, 40)))",
            {'twitter', 'facebook_ads'},
            TaxonTypeEnum.metric,
        ),
        # Errors
        ("iff(true)", set()),
        (
            "iff(fb_tw_merged_ad_id == 'id', twitter|ad_id, facebook_ads|spend)",
            {'twitter', 'facebook_ads'},
            TaxonTypeEnum.dimension,
        ),
        ("iff(generic_spend > 0, fb_tw_merged_ad_id)", {'twitter', 'facebook_ads'}, TaxonTypeEnum.dimension),
        ("iff(fb_tw_merged_ad_id == 'id', generic_cpm)", {'twitter', 'facebook_ads'}, TaxonTypeEnum.metric),
    ],
    ids=get_test_case_id,
)
def test_iff_render(request, test_input):
    execute_test(request, test_input)


@pytest.mark.parametrize(
    "test_input",
    [
        (
            "ifs(fb_tw_merged_ad_id == 'tw', twitter|ad_id, fb_tw_merged_ad_id == 'fb', facebook_ads|ad_id)",
            {'twitter', 'facebook_ads'},
            TaxonTypeEnum.dimension,
        ),
        (
            "ifs(fb_tw_merged_ad_id == 'tw', twitter|spend, fb_tw_merged_ad_id == 'fb', facebook_ads|spend)",
            {'twitter', 'facebook_ads'},
            TaxonTypeEnum.metric,
        ),
        (
            "ifs(generic_spend > 1, twitter|spend, generic_spend > 0, facebook_ads|spend, generic_spend)",
            {'twitter', 'facebook_ads'},
            TaxonTypeEnum.metric,
        ),
        (
            "ifs(generic_cpm > 1, twitter|spend, generic_cpm > 0, facebook_ads|spend, 42)",
            {'twitter', 'facebook_ads'},
            TaxonTypeEnum.metric,
        ),
        (
            "ifs(fb_tw_merged_ad_id == 'tw', 10, fb_tw_merged_ad_id == 'fb', 20, ?unknown == 'un', 30, 40)",
            {'twitter', 'facebook_ads'},
            TaxonTypeEnum.metric,
        ),
        ("ifs(true)", set()),
        (
            "ifs(fb_tw_merged_ad_id == 'tw', twitter|ad_id, fb_tw_merged_ad_id == 'fb', facebook_ads|spend)",
            {'twitter', 'facebook_ads'},
            TaxonTypeEnum.dimension,
        ),
        (
            "ifs(generic_spend > 1, twitter|ad_id, generic_spend > 0, facebook_ads|ad_id)",
            {'twitter', 'facebook_ads'},
            TaxonTypeEnum.dimension,
        ),
        ("ifs(1, twitter|ad_id, 2, facebook_ads|ad_id)", {'twitter', 'facebook_ads'}, TaxonTypeEnum.dimension),
        (
            "ifs(1, twitter|ad_id, 2, facebook_ads|ad_id, facebook_ads|ad_id)",
            {'twitter', 'facebook_ads'},
            TaxonTypeEnum.dimension,
        ),
        (
            "ifs(fb_tw_merged_ad_id == 'tw', twitter|spend, fb_tw_merged_ad_id == 'fb', facebook_ads|spend, generic_cpm)",
            {'twitter', 'facebook_ads'},
            TaxonTypeEnum.metric,
        ),
    ],
    ids=get_test_case_id,
)
def test_ifs_render(request, test_input):
    execute_test(request, test_input)


@pytest.mark.parametrize(
    "test_input",
    [('?facebook_ads|spend + twitter|spend', {'facebook_ads'}), ('?facebook_ads|spend + twitter|spend', {'twitter'})],
)
def test_optional_slugs(request, test_input):
    execute_test(request, test_input)


@pytest.mark.parametrize(
    "test_input",
    [
        (
            '''
                        iff(
                        fb_tw_merged_objective == "views",generic_spend,generic_spend*1.5)
                        ''',
            {'twitter', 'facebook_ads'},
        )
    ],
)
def test_metric_with_dim_calc(request, test_input):
    execute_test(request, test_input)


@pytest.mark.parametrize(
    "test_input",
    [
        ('city', {'bing'}),
        ('city', set()),
        ('merge(concat(facebook_ads|objective,"xx"),concat(twitter|objective,"yy"))', {'facebook_ads', 'twitter'}),
        ('merge(?twitter|ad_id, ?facebook_ads|ad_id)', {'twitter'}),
        ('merge(?twitter|ad_id, ?facebook_ads|ad_id)', {'twitter', 'facebook_ads'}),
        (
            'concat(merge(twitter|ad_id,facebook_ads|ad_id),merge(twitter|ad_name,facebook_ads|ad_name))',
            {'twitter', 'facebook_ads'},
        ),
        ('merge(tw_ad_id, fb_ad_id)', {'twitter', 'facebook_ads'}),
    ],
)
def test_simple_dim_calc(request, test_input):
    test_input = test_input + (TaxonTypeEnum.dimension,)
    execute_test(request, test_input)


@pytest.mark.parametrize(
    "test_input",
    [
        ('merge(1, ?twitter|ad_name, "hello")', {'twitter'}),
        ('merge(?twitter|ad_id, ?twitter|ad_name)', {'twitter'}),
        ('concat(?twitter|ad_id, ?twitter|ad_name, ?facebook_ads|ad_name)', {'twitter', 'facebook_ads'}),
    ],
)
def test_simple_dim_calc_errors(request, test_input):
    test_input = test_input + (TaxonTypeEnum.dimension,)
    execute_test(request, test_input)


@pytest.mark.parametrize(
    "test_input",
    [
        # Error cases
        ("convert_timezone()", set()),
        ("convert_timezone(twitter|date)", {'twitter'}),
        ("convert_timezone(twitter|spend, 'UTC', 'Europe/Prague')", {'twitter'}),
        ("convert_timezone(twitter|date, 'INVALID', 'INVALID TOO')", {'twitter'}),
        ("convert_timezone(?twitter|date, 'UTC')", {'facebook_ads'}),
        ("convert_timezone(?twitter|date, 'UTC', 'Europe/Prague')", {'facebook_ads'}),
        ("convert_timezone(twitter|date, 'UTC')", {'twitter'}),
        ("convert_timezone(twitter|date, 'UTC', 'Europe/Prague')", {'twitter'}),
        ("convert_timezone(merged_date, 'UTC', 'Europe/Prague')", {'twitter'}),
        ('convert_timezone(merged_date, "UTC")', {'twitter', 'facebook_ads'}),
        ('convert_timezone(merged_date, "UTC", "Europe/Prague")', {'twitter', 'facebook_ads'}),
    ],
    ids=get_test_case_id,
)
def test_convert_timezone(request, test_input):
    test_input = test_input + (TaxonTypeEnum.dimension,)
    execute_test(request, test_input)


@pytest.mark.parametrize(
    "test_input",
    [
        ('parse(fb_tw_merged_objective,"|",2)', {'twitter', 'facebook_ads'}),
        ('upper(fb_tw_merged_objective)', {'facebook_ads'}),
        ('trim(fb_tw_merged_objective)', {'facebook_ads'}),
    ],
)
def test_string_manipulation(request, test_input):
    test_input = test_input + (TaxonTypeEnum.dimension,)
    execute_test(request, test_input)


@pytest.mark.parametrize(
    "test_input",
    [
        # Error cases
        ("contains()", set()),
        ("contains(twitter|ad_id)", {'twitter'}),
        ("contains(twitter|spend, 'id')", {'twitter'}),  # 1. argument is not a dimension
        ("contains(twitter|ad_id, 'id', twitter|ad_id)", {'twitter'}),  # 3. argument is not a constant
        ("contains(?twitter|ad_id, 'id')", set(), TaxonTypeEnum.dimension),  # Invalid dimension taxon
        # Valid cases
        # Test escaping of special characters
        ("contains(lower(twitter|ad_id), 'text_asd')", {'twitter'}, TaxonTypeEnum.dimension),
        ("contains(campaign_id2, 'bud79')", {'twitter', 'facebook_ads'}, TaxonTypeEnum.dimension),
        ("contains(twitter|ad_id, 'id1', 'id2')", {'twitter'}, TaxonTypeEnum.dimension),
        ("contains(fb_tw_merged_ad_id, 'id1', 'id2')", {'twitter', 'facebook_ads'}, TaxonTypeEnum.dimension),
        ("iff(contains(twitter|ad_id, 'id1', 'id2'), spend, 0)", {'twitter'}, TaxonTypeEnum.metric),
        (
            "iff(contains(fb_tw_merged_ad_id, 'id1', 'id2'), spend, 0)",
            {'twitter', 'facebook_ads'},
            TaxonTypeEnum.metric,
        ),
    ],
)
def test_contains_render(request, test_input):
    execute_test(request, test_input)


@pytest.mark.parametrize(
    "test_input",
    [
        # Error cases
        ("year()", set()),
        ("year(?twitter|spend)", set()),
        ("year(twitter|spend)", {'twitter'}),
        # Valid cases
        ("date_hour(twitter|date)", {'twitter'}, TaxonTypeEnum.dimension),
        ("date(twitter|date)", {'twitter'}, TaxonTypeEnum.dimension),
        ("date_week(twitter|date)", {'twitter'}, TaxonTypeEnum.dimension),
        ("date_month(twitter|date)", {'twitter'}, TaxonTypeEnum.dimension),
        ("hour_of_day(twitter|date)", {'twitter'}, TaxonTypeEnum.dimension),
        ("day_of_week(twitter|date)", {'twitter'}, TaxonTypeEnum.dimension),
        ("week_of_year(twitter|date)", {'twitter'}, TaxonTypeEnum.dimension),
        ("month_of_year(twitter|date)", {'twitter'}, TaxonTypeEnum.dimension),
        ("year(twitter|date)", {'twitter'}, TaxonTypeEnum.dimension),
        ("year(merged_date)", {'twitter', 'facebook_ads'}, TaxonTypeEnum.dimension),
    ],
)
def test_single_datetime_dimension_functions_render(request, test_input):
    execute_test(request, test_input)


@pytest.mark.parametrize(
    "test_input",
    [
        # Error cases
        ("to_text()", set(), TaxonTypeEnum.dimension),
        ("to_bool()", set(), TaxonTypeEnum.dimension),
        ("to_bool(date)", set(), TaxonTypeEnum.dimension),
        ("to_number()", set(), TaxonTypeEnum.metric),
        ("to_number(date)", set(), TaxonTypeEnum.metric),
        ("to_number(wetransfer_product_name,fb_tw_spend_all_optional)", {'twitter'}, TaxonTypeEnum.metric),
        ("to_date(wetransfer_product_name)", set(), TaxonTypeEnum.dimension),
        ("to_date(facebook_ads|done)", {'facebook_ads'}, TaxonTypeEnum.dimension),
        ("to_date(fb_tw_spend_all_optional, 'YYYY-MM-DD')", {'twitter'}, TaxonTypeEnum.dimension),
        ("to_text(twitter|date)", {'twitter'}, TaxonTypeEnum.dimension),
        ("to_text(twitter|spend)", {'twitter'}, TaxonTypeEnum.dimension),
        ("to_bool(facebook_ads|done)", {'facebook_ads'}, TaxonTypeEnum.dimension),
        ("to_bool(spend)", set(), TaxonTypeEnum.dimension),
        ("to_bool(ad_name)", set(), TaxonTypeEnum.dimension),
        ("to_date(spend)", set(), TaxonTypeEnum.dimension),
        ("to_date(ad_name, 'YYYY-MM-DD')", set(), TaxonTypeEnum.dimension),
        ("to_date(facebook_ads|date, 'YYYY-MM-DD')", {'facebook_ads'}, TaxonTypeEnum.dimension),
    ],
    ids=get_test_case_id,
)
def test_cast_functions_render(request, test_input):
    execute_test(request, test_input)


@pytest.mark.parametrize(
    "test_input",
    [
        # Error cases
        ("date_diff()", set()),
        ("date_diff('SECOND')", set()),
        ("date_diff('SECOND', twitter|date)", {'twitter'}),
        ("date_diff('SECOND', twitter|date, facebook_ads|date)", {'twitter', 'facebook_ads'}),
        ("date_diff('INVALID', twitter|date, twitter|date)", {'twitter'}),
        ("date_diff(twitter|ad_id, twitter|date, twitter|date)", {'twitter'}),
        ("date_diff('SECOND', twitter|date, twitter|spend)", {'twitter'}),
        # Valid cases
        ("date_diff('SECOND', ?twitter|date, ?twitter|date)", {'facebook_ads'}, TaxonTypeEnum.dimension),
        ("date_diff('SECOND', twitter|date, twitter|date)", {'twitter'}, TaxonTypeEnum.dimension),
        ("date_diff('SECOND', twitter|date, merged_date)", {'twitter'}, TaxonTypeEnum.dimension),
        ("date_diff('MINUTE', twitter|date, merged_date)", {'twitter', 'facebook_ads'}, TaxonTypeEnum.dimension),
        ("date_diff('YEAR', merged_date, merged_date)", {'twitter', 'facebook_ads'}, TaxonTypeEnum.dimension),
    ],
    ids=get_test_case_id,
)
def test_date_diff_render(request, test_input):
    execute_test(request, test_input)


@pytest.mark.parametrize(
    "test_input",
    [
        # Valid simple cases
        ("iff(twitter|spend == 0, twitter|spend)", {'twitter'}, TaxonTypeEnum.metric),
        ("iff(twitter|spend != 0, twitter|spend)", {'twitter'}, TaxonTypeEnum.metric),
        ("iff(twitter|spend > 0, twitter|spend)", {'twitter'}, TaxonTypeEnum.metric),
        ("iff(twitter|spend >= 0, twitter|spend)", {'twitter'}, TaxonTypeEnum.metric),
        ("iff(twitter|spend < 0, twitter|spend)", {'twitter'}, TaxonTypeEnum.metric),
        ("iff(twitter|spend <= 0, twitter|spend)", {'twitter'}, TaxonTypeEnum.metric),
        ("iff(true || false, twitter|spend)", {'twitter'}, TaxonTypeEnum.metric),
        ("iff(true && false, twitter|spend)", {'twitter'}, TaxonTypeEnum.metric),
        ("iff(not true, twitter|spend)", {'twitter'}, TaxonTypeEnum.metric),
        ("iff(not not false, twitter|spend)", {'twitter'}, TaxonTypeEnum.metric),
        ("iff(twitter|spend is null, twitter|spend)", {'twitter'}, TaxonTypeEnum.metric),
        ("iff(twitter|spend is not null, twitter|spend)", {'twitter'}, TaxonTypeEnum.metric),
        # Valid complex cases
        (
            "iff((twitter|spend > 0) || (facebook_ads|spend > 0), twitter|spend)",
            {'twitter', 'facebook_ads'},
            TaxonTypeEnum.metric,
        ),
        (
            "iff(contains(fb_tw_merged_ad_id, 'id1', 'id2') && contains(fb_tw_merged_ad_id, 'id3', 'id4'), fb_tw_merged_ad_id)",
            {'twitter', 'facebook_ads'},
            TaxonTypeEnum.dimension,
        ),
        (
            "iff(((generic_spend == 0) || (generic_spend != 0)) && true, generic_spend)",
            {'twitter', 'facebook_ads'},
            TaxonTypeEnum.metric,
        ),
        (
            "iff(not (not true) && not (false || false) && not (generic_spend != 0), generic_spend)",
            {'twitter', 'facebook_ads'},
            TaxonTypeEnum.metric,
        ),
        (
            "iff(NOT (generic_spend IS NOT NULL) && (generic_spend IS NULL) && TRUE || FALSE, generic_spend)",
            {'twitter', 'facebook_ads'},
            TaxonTypeEnum.metric,
        ),
    ],
    ids=get_test_case_id,
)
def test_operators_render(request, test_input):
    execute_test(request, test_input)


@pytest.mark.parametrize(
    "test_input",
    [
        # error cases
        ("override()", {'twitter', 'facebook_ads'}, TaxonTypeEnum.dimension),
        ("override(fb_tw_merged_ad_id)", {'twitter', 'facebook_ads'}, TaxonTypeEnum.dimension),
        ("override(fb_tw_merged_ad_id, fb_tw_merged_ad_id)", {'twitter', 'facebook_ads'}, TaxonTypeEnum.dimension),
        ("override(fb_tw_merged_ad_id, 'override-mapping-slug')", {'twitter'}, TaxonTypeEnum.dimension),
        ("override(fb_tw_merged_ad_id, 'override-mapping-slug', true)", {'twitter'}, TaxonTypeEnum.dimension),
        ("override(fb_tw_merged_ad_id, 'override-mapping-slug', false)", {'twitter'}, TaxonTypeEnum.dimension),
        (
            "concat(concat(concat(override(fb_tw_merged_ad_id, 'override-mapping-slug'), 's'), 'v'), '-a')",
            {'twitter'},
            TaxonTypeEnum.dimension,
        ),
        (
            "concat(concat(concat(override(fb_tw_merged_ad_id, 'override-mapping-slug'), 's'), override(fb_tw_merged_ad_id, 'override-mapping-slug-2')), '-a')",
            {'twitter', 'facebook_ads'},
            TaxonTypeEnum.dimension,
        ),
    ],
    ids=get_test_case_id,
)
def test_override_render(request, test_input):
    execute_test(request, test_input)


@pytest.mark.parametrize(
    "test_input",
    [
        ("-0.07", {'twitter'}, TaxonTypeEnum.metric, 'taxon_slug'),
        ("007", {'twitter'}, TaxonTypeEnum.metric, 'taxon_slug'),
        ("'Bond, James Bond'", {'twitter'}, TaxonTypeEnum.dimension, 'taxon_slug'),
        ('"Shaken, not stirred"', {'twitter'}, TaxonTypeEnum.dimension, 'taxon_slug'),
        ("true", {'twitter'}, TaxonTypeEnum.dimension, 'taxon_slug'),
        ("false", {'twitter'}, TaxonTypeEnum.dimension, 'taxon_slug'),
    ],
    ids=get_test_case_id,
)
def test_constants_render(request, test_input):
    execute_test(request, test_input)


@pytest.mark.parametrize(
    "test_input",
    [
        # Error cases
        ("overall()", {'twitter'}, TaxonTypeEnum.metric),
        ("overall(twitter|ad_id)", {'twitter'}, TaxonTypeEnum.metric),
        # Valid cases
        ("overall(twitter|spend)", {'twitter'}, TaxonTypeEnum.metric),
        ("overall(generic_spend)", {'twitter'}, TaxonTypeEnum.metric),
        ("overall(generic_spend)", {'twitter', 'facebook_ads'}, TaxonTypeEnum.metric),
    ],
    ids=get_test_case_id,
)
def test_overall_render(request, test_input):
    execute_test(request, test_input)


@pytest.mark.parametrize(
    "test_input",
    [
        # Error cases
        ("cumulative()", {'twitter'}, TaxonTypeEnum.metric),
        ("cumulative(twitter|ad_id, twitter|date)", {'twitter'}, TaxonTypeEnum.metric),
        ("cumulative(twitter|spend, twitter|spend)", {'twitter'}, TaxonTypeEnum.metric),
        # Valid cases
        ("cumulative(twitter|spend, twitter|date)", {'twitter'}, TaxonTypeEnum.metric),
        ("cumulative(generic_spend, date)", {'twitter'}, TaxonTypeEnum.metric),
        ("cumulative(generic_spend, date)", {'twitter', 'facebook_ads'}, TaxonTypeEnum.metric),
        # Optimization cases
        ("cumulative(42 / generic_spend, date)", {'twitter', 'facebook_ads'}, TaxonTypeEnum.metric),
        ("cumulative(generic_spend / 42, date)", {'twitter', 'facebook_ads'}, TaxonTypeEnum.metric),
        ("cumulative(generic_spend / generic_impressions, date)", {'twitter', 'facebook_ads'}, TaxonTypeEnum.metric),
        (
            "cumulative((generic_spend * 42) / generic_impressions, date)",
            {'twitter', 'facebook_ads'},
            TaxonTypeEnum.metric,
        ),
        (
            "cumulative(generic_spend / (generic_impressions * 42), date)",
            {'twitter', 'facebook_ads'},
            TaxonTypeEnum.metric,
        ),
        (
            "cumulative((generic_spend / generic_impressions) / 42, date)",
            {'twitter', 'facebook_ads'},
            TaxonTypeEnum.metric,
        ),
        ("cumulative(generic_cpm, date)", {'twitter', 'facebook_ads'}, TaxonTypeEnum.metric),
        ("cumulative(generic_cpm / 42, date)", {'twitter', 'facebook_ads'}, TaxonTypeEnum.metric),
        ("coalesce(cumulative(generic_cpm / 42, date), 0)", {'twitter', 'facebook_ads'}, TaxonTypeEnum.metric),
    ],
    ids=get_test_case_id,
)
def test_cumulative_render(request, test_input):
    execute_test(request, test_input)


@pytest.mark.parametrize(
    "test_input",
    [('?required_spend + 5', {'facebook_ads'}), ('?required_spend + 5', {'facebook_ads', 'adwords', 'twitter'})],
)
def test_optional_taxon_render(request, test_input):
    execute_test(request, test_input)
