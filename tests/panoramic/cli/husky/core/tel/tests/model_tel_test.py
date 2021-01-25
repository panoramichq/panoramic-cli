import pytest

from panoramic.cli.husky.core.model.enums import ModelVisibility
from panoramic.cli.husky.core.model.models import HuskyModel
from panoramic.cli.husky.core.sql_alchemy_util import compile_query
from panoramic.cli.husky.core.tel.tel_dialect import ModelTelDialect
from panoramic.cli.husky.service.context import SNOWFLAKE_HUSKY_CONTEXT

_TMP_MODEL = HuskyModel(
    {
        'name': 'test_model',
        'fully_qualified_name_parts': ['pds', 'database_a', 'company_a', 'table_a'],
        'attributes': {
            'ds_1|ad_id': {'tel_transformation': '"col_ad_id"', 'taxon': 'ds_1|ad_id', 'identifier': True},
            'ds_1|gender': {'tel_transformation': '"col_gender"', 'taxon': 'ds_1|gender', 'identifier': False},
            'ds_1|spend': {'tel_transformation': '"col_spend"', 'taxon': 'ds_1|spend', 'identifier': False},
            'ds_1|start_time': {
                'tel_transformation': '"col_start_time"',
                'taxon': 'ds_1|start_time',
                'identifier': False,
            },
            'ds_1|end_time': {'tel_transformation': '"col_end_time"', 'taxon': 'ds_1|end_time', 'identifier': False},
        },
        'data_sources': ['ds_1'],
        'company_id': 'company-id',
        'visibility': ModelVisibility.available,
    },
    strict=True,
)


# on purpose, some of the inputs do not make sense (e.g. 10 + true)
# this highlights the fact that TEL only validates that it understands the expression
# (there's no guarantee that the final SQL query will run)
@pytest.mark.parametrize(
    ['inp', 'expectation'],
    [
        ('1+10', '11'),
        (
            '1+10/gender',
            'coalesce(1, 0) + coalesce(10 / nullif(database_a_company_a_table_a_0c1c3f7d9ae4c141.col_gender, 0), 0)',
        ),
        ('1 * (10 - 123)', '-113'),
        (
            '1 / ("column_1" - 123)',
            '1 / nullif((coalesce(database_a_company_a_table_a_0c1c3f7d9ae4c141.column_1, 0) - coalesce(123, 0)), 0)',
        ),
        ('\'my constant\'', '\'my constant\''),
        ('"col_spend" * 10', 'database_a_company_a_table_a_0c1c3f7d9ae4c141.col_spend * 10'),
        ('10 + true', '11'),
        ('123 + 456.789', '579.789'),
        ('123 + 456.789 / 10', '168.6789'),
        ('true || (false && true)', 'true'),
        (
            '"col_spend" == 10 || (ad_id > 1 && (1 + 4 == \'constant\'))',
            (
                'database_a_company_a_table_a_0c1c3f7d9ae4c141.col_spend = 10 OR (database_a_company_a_table_a_0c1c3f7d9ae4c141.col_ad_id > 1'
                ' AND (false))'
            ),
        ),
        ('trim(\'string\')', 'trim(\'string\')'),
        ('trim(gender)', 'trim(database_a_company_a_table_a_0c1c3f7d9ae4c141.col_gender)'),
        ('upper(\'string\')', 'upper(\'string\')'),
        ('upper(gender)', 'upper(database_a_company_a_table_a_0c1c3f7d9ae4c141.col_gender)'),
        ('lower(\'string\')', 'lower(\'string\')'),
        ('lower(gender)', 'lower(database_a_company_a_table_a_0c1c3f7d9ae4c141.col_gender)'),
        ('to_bool("col_gender")', 'CAST(database_a_company_a_table_a_0c1c3f7d9ae4c141.col_gender AS BOOLEAN)'),
        ('to_bool(ad_id)', 'CAST(database_a_company_a_table_a_0c1c3f7d9ae4c141.col_ad_id AS BOOLEAN)'),
        ('to_text("col_gender")', 'CAST(database_a_company_a_table_a_0c1c3f7d9ae4c141.col_gender AS VARCHAR)'),
        ('to_text(ad_id)', 'CAST(database_a_company_a_table_a_0c1c3f7d9ae4c141.col_ad_id AS VARCHAR)'),
        ('to_date("col_gender")', 'to_date(database_a_company_a_table_a_0c1c3f7d9ae4c141.col_gender)'),
        (
            'to_date(ad_id, \'YYYY-MM-DD\')',
            'to_date(database_a_company_a_table_a_0c1c3f7d9ae4c141.col_ad_id, \'YYYY-MM-DD\')',
        ),
        (
            'concat("col_gender", ad_id, \'constant\')',
            'concat(database_a_company_a_table_a_0c1c3f7d9ae4c141.col_gender, database_a_company_a_table_a_0c1c3f7d9ae4c141.col_ad_id, \'constant\')',
        ),
        (
            'parse("col_gender", \'constant\', 4)',
            'split_part(database_a_company_a_table_a_0c1c3f7d9ae4c141.col_gender, \'constant\', 4)',
        ),
        ('to_number(ad_id)', 'CAST(database_a_company_a_table_a_0c1c3f7d9ae4c141.col_ad_id AS NUMERIC(16))'),
        (
            'to_number("col_gender", 10)',
            'CAST(database_a_company_a_table_a_0c1c3f7d9ae4c141.col_gender AS DECIMAL(16, 10))',
        ),
        ('year(ad_id)', "date_part('YEAR', database_a_company_a_table_a_0c1c3f7d9ae4c141.col_ad_id)"),
        ('hour_of_day(spend)', "date_part('HOUR', database_a_company_a_table_a_0c1c3f7d9ae4c141.col_spend)"),
        ('week_of_year("col_gender")', "date_part('WEEK', database_a_company_a_table_a_0c1c3f7d9ae4c141.col_gender)"),
        (
            'day_of_week("col_start_time")',
            "date_part('DOW', database_a_company_a_table_a_0c1c3f7d9ae4c141.col_start_time)",
        ),
        (
            'iff("col_start_time" == \'2020-01-01\', gender)',
            (
                "CASE WHEN (database_a_company_a_table_a_0c1c3f7d9ae4c141.col_start_time = '2020-01-01') THEN "
                "database_a_company_a_table_a_0c1c3f7d9ae4c141.col_gender ELSE NULL END"
            ),
        ),
        (
            'iff("col_start_time" == \'2020-01-01\', gender, spend + 10)',
            (
                "CASE WHEN (database_a_company_a_table_a_0c1c3f7d9ae4c141.col_start_time = '2020-01-01') THEN "
                "database_a_company_a_table_a_0c1c3f7d9ae4c141.col_gender ELSE "
                "coalesce(database_a_company_a_table_a_0c1c3f7d9ae4c141.col_spend, 0) + coalesce(10, 0) END"
            ),
        ),
        (
            'convert_timezone("col_start_time", \'tz-1\')',
            "convert_timezone('tz-1', database_a_company_a_table_a_0c1c3f7d9ae4c141.col_start_time)",
        ),
        (
            'convert_timezone("col_start_time", \'tz-1\', \'tz-2\')',
            "convert_timezone('tz-1', 'tz-2', database_a_company_a_table_a_0c1c3f7d9ae4c141.col_start_time)",
        ),
        (
            'date_trunc(start_time, \'HOUR\')',
            'date_trunc(\'HOUR\', database_a_company_a_table_a_0c1c3f7d9ae4c141.col_start_time)',
        ),
        (
            'date_diff(\'HOUR\', start_time, end_time)',
            (
                'timestampdiff(HOUR, database_a_company_a_table_a_0c1c3f7d9ae4c141.col_start_time,'
                ' database_a_company_a_table_a_0c1c3f7d9ae4c141.col_end_time)'
            ),
        ),
        (
            'contains(start_time, \'txt\', \'my-text2\')',
            (
                "database_a_company_a_table_a_0c1c3f7d9ae4c141.col_start_time LIKE '%%txt%%' ESCAPE '/' OR "
                "database_a_company_a_table_a_0c1c3f7d9ae4c141.col_start_time LIKE '%%my-text2%%' ESCAPE '/'"
            ),
        ),
        (
            'ifs("col_start_time" == \'2020-01-01\', gender, "col_start_time" == \'2020-01-02\', spend + 10)',
            (
                "CASE WHEN (database_a_company_a_table_a_0c1c3f7d9ae4c141.col_start_time = '2020-01-01') THEN "
                "database_a_company_a_table_a_0c1c3f7d9ae4c141.col_gender WHEN "
                "(database_a_company_a_table_a_0c1c3f7d9ae4c141.col_start_time = '2020-01-02') THEN "
                "coalesce(database_a_company_a_table_a_0c1c3f7d9ae4c141.col_spend, 0) + coalesce(10, 0) ELSE 0 "
                "END"
            ),
        ),
        (
            'ifs("col_start_time" == \'2020-01-01\', gender, "col_start_time" == \'2020-01-02\', spend + 10, ad_id)',
            (
                "CASE WHEN (database_a_company_a_table_a_0c1c3f7d9ae4c141.col_start_time = '2020-01-01') THEN "
                "database_a_company_a_table_a_0c1c3f7d9ae4c141.col_gender WHEN "
                "(database_a_company_a_table_a_0c1c3f7d9ae4c141.col_start_time = '2020-01-02') THEN "
                "coalesce(database_a_company_a_table_a_0c1c3f7d9ae4c141.col_spend, 0) + coalesce(10, 0) ELSE "
                "database_a_company_a_table_a_0c1c3f7d9ae4c141.col_ad_id END"
            ),
        ),
        ('now()', 'now()'),
    ],
)
def test_render_visitor(inp, expectation):
    result = ModelTelDialect(
        unique_object_name=_TMP_MODEL.unique_object_name(SNOWFLAKE_HUSKY_CONTEXT),
        virtual_data_source=_TMP_MODEL.data_sources[0],
        model=_TMP_MODEL,
    ).render(inp, SNOWFLAKE_HUSKY_CONTEXT, {})
    assert compile_query(result.sql(SNOWFLAKE_HUSKY_CONTEXT.dialect)) == expectation
