import pytest
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect

from panoramic.cli.datacol.tztools import now
from panoramic.cli.husky.core.taxonomy.override_mapping.enums import MappingSourceType
from panoramic.cli.husky.core.taxonomy.override_mapping.models import OverrideMapping
from panoramic.cli.husky.service.blending.features.override_mapping.sql import (
    OverrideMappingSql,
)
from tests.panoramic.cli.husky.test.test_base import BaseTest


@pytest.fixture
def mapping_definition():
    yield OverrideMapping(
        slug='slug-1',
        name='My mapping',
        company_id='company_id',
        definition=[['orig_1', 'changed_1'], ['orig_2', 'changed_2'], [None, 'changed_3'], ['orig_4', None]],
        source_type=MappingSourceType.DIRECT,
        created_by='user',
        created_at=now(),
    )


class TestRenderDirectMappingCte(BaseTest):
    @staticmethod
    def _prepare_sql(dialect):
        mapping_definition = OverrideMapping(
            slug='slug-1',
            name='My mapping',
            company_id='company_id',
            definition=[['orig_1', 'changed_1'], ['orig_2', 'changed_2'], [None, 'changed_3'], ['orig_4', None]],
            source_type=MappingSourceType.DIRECT,
            created_by='user',
            created_at=now(),
        )

        query = OverrideMappingSql.render_direct_mapping(mapping_definition)
        return str(query.compile(compile_kwargs={"literal_binds": True}, dialect=dialect()))

    def test_snowflake_dialect(self):
        sql = self._prepare_sql(SnowflakeDialect)

        self.write_test_expectations('query.sql', str(sql))
        expected_query = self.read_test_expectations('query.sql')
        self.assertEqual(expected_query, str(sql))


@pytest.mark.parametrize(
    'include_unknown_values,expected',
    [(True, '__om_my_column_slug_1_true_a576d893f201e0e2'), (False, '__om_my_column_slug_1_false_5e6b4360696a348f')],
)
def test_generate_identifier(mapping_definition, include_unknown_values, expected):
    identifier = OverrideMappingSql.generate_identifier('my_column', mapping_definition.slug, include_unknown_values)
    assert identifier == expected


def test_generate_cte_name(mapping_definition):
    identifier = OverrideMappingSql.generate_cte_name(mapping_definition.slug)
    assert identifier == '__om_slug_1_609591aff3bc337e'
