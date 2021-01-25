from sqlalchemy.sql import Select, column, table

from panoramic.cli.husky.core.sql_alchemy_util import compile_query
from panoramic.cli.husky.service.blending.dataframe_joins import blend_dataframes
from panoramic.cli.husky.service.constants import HUSKY_QUERY_DATA_SOURCE_COLUMN_NAME
from panoramic.cli.husky.service.context import SNOWFLAKE_HUSKY_CONTEXT
from panoramic.cli.husky.service.types.types import Dataframe
from tests.panoramic.cli.husky.test.mocks.mock_dataframe import (
    get_mocked_dataframe_columns_map,
)
from tests.panoramic.cli.husky.test.test_base import BaseTest


class TestDataframeBlending(BaseTest):
    def test_blending_2_0(self):
        q1 = Select(
            columns=[column('objective'), column('impressions'), column(HUSKY_QUERY_DATA_SOURCE_COLUMN_NAME)],
            from_obj=table('table1'),
        )
        df1 = Dataframe(q1, get_mocked_dataframe_columns_map(['objective', 'impressions']), set(), {'SF'})
        q2 = Select(
            columns=[column('age'), column('impressions'), column(HUSKY_QUERY_DATA_SOURCE_COLUMN_NAME)],
            from_obj=table('table2'),
        )
        df2 = Dataframe(q2, get_mocked_dataframe_columns_map(['age_bucket', 'impressions']), set(), {'SF'})
        blended_df = blend_dataframes(SNOWFLAKE_HUSKY_CONTEXT, [df1, df2])
        self.write_test_expectations('query.sql', compile_query(blended_df.query))
        expected_query = self.read_test_expectations('query.sql')
        self.assertEqual(expected_query, compile_query(blended_df.query))
        self.assertEqual({'objective', 'age_bucket', 'impressions'}, set(blended_df.slug_to_column.keys()))

    def test_blending_2_1(self):
        q1 = Select(
            columns=[column('ad_id'), column('impressions'), column(HUSKY_QUERY_DATA_SOURCE_COLUMN_NAME)],
            from_obj=table('table1'),
        )
        df1 = Dataframe(q1, get_mocked_dataframe_columns_map(['ad_id', 'impressions']), {'model_name_a'}, {'SF'})
        q2 = Select(
            columns=[column('ad_id'), column('impressions'), column(HUSKY_QUERY_DATA_SOURCE_COLUMN_NAME)],
            from_obj=table('table2'),
        )
        df2 = Dataframe(q2, get_mocked_dataframe_columns_map(['ad_id', 'impressions']), {'model_name_b'}, {'SF'})
        blended_df = blend_dataframes(SNOWFLAKE_HUSKY_CONTEXT, [df1, df2])
        self.write_test_expectations('query.sql', compile_query(blended_df.query))
        expected_query = self.read_test_expectations('query.sql')
        self.assertEqual(expected_query, compile_query(blended_df.query))
        self.assertEqual({'ad_id', 'impressions'}, set(blended_df.slug_to_column.keys()))
        self.assertEqual({'model_name_a', 'model_name_b'}, set(blended_df.used_model_names))

    def test_blending_2_2(self):
        q1 = Select(
            columns=[column('ad_id'), column('impressions'), column(HUSKY_QUERY_DATA_SOURCE_COLUMN_NAME)],
            from_obj=table('table1'),
        )
        df1 = Dataframe(q1, get_mocked_dataframe_columns_map(['ad_id', 'impressions']), set(), {'SF'})
        q2 = Select(
            columns=[
                column('ad_id'),
                column('campaign_id'),
                column('impressions'),
                column(HUSKY_QUERY_DATA_SOURCE_COLUMN_NAME),
            ],
            from_obj=table('table2'),
        )
        df2 = Dataframe(q2, get_mocked_dataframe_columns_map(['ad_id', 'impressions', 'campaign_id']), set(), {'SF'})
        blended_df = blend_dataframes(SNOWFLAKE_HUSKY_CONTEXT, [df1, df2])
        self.write_test_expectations('query.sql', compile_query(blended_df.query))
        expected_query = self.read_test_expectations('query.sql')
        self.assertEqual(expected_query, compile_query(blended_df.query))
        self.assertEqual({'ad_id', 'impressions', 'campaign_id'}, set(blended_df.slug_to_column.keys()))

    def test_blending_3_1(self):
        q1 = Select(
            columns=[column('ad_id'), column('impressions'), column(HUSKY_QUERY_DATA_SOURCE_COLUMN_NAME)],
            from_obj=table('table1'),
        )
        df1 = Dataframe(q1, get_mocked_dataframe_columns_map(['ad_id', 'impressions']), set(), {'SF'})
        q2 = Select(
            columns=[
                column('ad_id'),
                column('campaign_id'),
                column('impressions'),
                column(HUSKY_QUERY_DATA_SOURCE_COLUMN_NAME),
            ],
            from_obj=table('table2'),
        )
        df2 = Dataframe(q2, get_mocked_dataframe_columns_map(['ad_id', 'impressions', 'campaign_id']), set(), {'SF'})
        q3 = Select(
            columns=[
                column('ad_id'),
                column('campaign_id'),
                column('impressions'),
                column(HUSKY_QUERY_DATA_SOURCE_COLUMN_NAME),
            ],
            from_obj=table('table3'),
        )
        df3 = Dataframe(q3, get_mocked_dataframe_columns_map(['ad_id', 'impressions', 'campaign_id']), set(), {'SF'})
        blended_df = blend_dataframes(SNOWFLAKE_HUSKY_CONTEXT, [df1, df2, df3])
        self.write_test_expectations('query.sql', compile_query(blended_df.query))
        expected_query = self.read_test_expectations('query.sql')
        self.assertEqual(expected_query, compile_query(blended_df.query))
        self.assertEqual({'ad_id', 'impressions', 'campaign_id'}, set(blended_df.slug_to_column.keys()))
