import itertools
from typing import Dict

from sqlalchemy import literal_column, select

from panoramic.cli.husky.service.context import SNOWFLAKE_HUSKY_CONTEXT
from panoramic.cli.husky.service.filter_builder.enums import (
    ArrayFilterOperator,
    FilterClauseType,
    LogicalOperator,
    SimpleFilterOperator,
)
from panoramic.cli.husky.service.filter_builder.filter_clauses import (
    GROUP_OPERATORS_FUNCTIONS,
    SIMPLE_OPERATORS_FUNCTIONS,
    GroupFilterClause,
    TaxonArrayFilterClause,
    TaxonTaxonFilterClause,
    TaxonValueFilterClause,
    UnknownOperator,
    generate_group_filter_clause_dict,
    generate_simple_filter_clause_dict,
)
from panoramic.cli.husky.service.select_builder.taxon_model_info import TaxonModelInfo
from tests.panoramic.cli.husky.test.mocks.husky_model import get_mock_husky_model
from tests.panoramic.cli.husky.test.test_base import BaseTest

TAXON_MODEL_INFO_MAP: Dict[str, TaxonModelInfo] = {
    'campaign': TaxonModelInfo('path.to.schema.campaign', get_mock_husky_model().name),
    'test': TaxonModelInfo('path.to.schema.test', get_mock_husky_model().name),
    'test_next': TaxonModelInfo('path.to.schema.test_next', get_mock_husky_model().name),
    'test_other': TaxonModelInfo('path.to.schema.test_other', get_mock_husky_model().name),
}


class FilterClauseTests(BaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.query = select(
            [
                literal_column('test'),
                literal_column('test_next'),
                literal_column('test_other'),
            ]
        )

    def test_taxon_value_generate(self):
        for operation in SIMPLE_OPERATORS_FUNCTIONS.keys():
            with self.subTest(f'Taxon value clause test with operator {operation.value}'):
                actual = TaxonValueFilterClause(
                    {
                        'type': FilterClauseType.TAXON_VALUE.value,
                        'taxon': 'test',
                        'operator': operation.value,
                        'value': '10',
                    }
                ).generate(SNOWFLAKE_HUSKY_CONTEXT, self.query, TAXON_MODEL_INFO_MAP)
                expected = f'path.to.schema.test {operation.value} :param_1'
                self.assertEqual(expected, str(actual))

        for operation in [SimpleFilterOperator.LIKE, SimpleFilterOperator.NOT_LIKE]:
            with self.subTest(f'Taxon value clause test with operator {operation.value}'):
                actual = TaxonValueFilterClause(
                    {
                        'type': FilterClauseType.TAXON_VALUE.value,
                        'taxon': 'test',
                        'operator': operation.value,
                        'value': '10',
                    }
                ).generate(SNOWFLAKE_HUSKY_CONTEXT, self.query, TAXON_MODEL_INFO_MAP)
                expected = (
                    f"CAST(path.to.schema.test AS VARCHAR) {operation.value.replace('_', ' ')} :param_1 ESCAPE '/'"
                )
                self.assertEqual(expected, str(actual))

        for operation in [SimpleFilterOperator.ILIKE, SimpleFilterOperator.NOT_ILIKE]:
            with self.subTest(f'Taxon value clause test with operator {operation.value}'):
                actual = TaxonValueFilterClause(
                    {
                        'type': FilterClauseType.TAXON_VALUE.value,
                        'taxon': 'test',
                        'operator': operation.value,
                        'value': '10',
                    }
                ).generate(SNOWFLAKE_HUSKY_CONTEXT, self.query, TAXON_MODEL_INFO_MAP)
                expected = f"lower(CAST(path.to.schema.test AS VARCHAR)) {operation.value.replace('_', ' ').replace('ILIKE', 'LIKE')} lower(:param_1) ESCAPE '/'"
                self.assertEqual(expected, str(actual))

        with self.subTest('Taxon value clause test with operator "IS NULL"'):
            actual = TaxonValueFilterClause(
                {
                    'type': FilterClauseType.TAXON_VALUE.value,
                    'taxon': 'test',
                    'operator': SimpleFilterOperator.EQ.value,
                    'value': None,
                }
            ).generate(SNOWFLAKE_HUSKY_CONTEXT, self.query, TAXON_MODEL_INFO_MAP)
            expected = 'path.to.schema.test IS NULL'
            self.assertEqual(expected, str(actual))

        with self.subTest('Taxon value clause test with operator "IS NOT NULL"'):
            actual = TaxonValueFilterClause(
                {
                    'type': FilterClauseType.TAXON_VALUE.value,
                    'taxon': 'test',
                    'operator': SimpleFilterOperator.NEQ,
                    'value': None,
                }
            ).generate(SNOWFLAKE_HUSKY_CONTEXT, self.query, TAXON_MODEL_INFO_MAP)
            expected = 'path.to.schema.test IS NOT NULL'
            self.assertEqual(expected, str(actual))

        with self.subTest('Taxon value clause test with operator "=" and empty string'):
            actual = TaxonValueFilterClause(
                {
                    'type': FilterClauseType.TAXON_VALUE.value,
                    'taxon': 'test',
                    'operator': SimpleFilterOperator.EQ.value,
                    'value': '',
                }
            ).generate(SNOWFLAKE_HUSKY_CONTEXT, self.query, TAXON_MODEL_INFO_MAP)
            expected = 'path.to.schema.test = :param_1'
            self.assertEqual(expected, str(actual))

    def test_taxon_value_clause_none_wrong(self):

        all_operators = itertools.chain(
            SIMPLE_OPERATORS_FUNCTIONS.keys(),
            [
                SimpleFilterOperator.LIKE,
                SimpleFilterOperator.NOT_LIKE,
                SimpleFilterOperator.ILIKE,
                SimpleFilterOperator.NOT_ILIKE,
            ],
        )
        for operation in all_operators:
            with self.subTest(f'Taxon value clause with None & operator {operation.value} is not supported'):

                filter_clause = TaxonValueFilterClause(
                    {
                        'type': FilterClauseType.TAXON_VALUE.value,
                        'taxon': 'test',
                        'operator': operation.value,
                        'value': None,
                    }
                )
                if operation in {SimpleFilterOperator.EQ, SimpleFilterOperator.NEQ}:
                    # these operators are allowed so this should not raise an exception
                    filter_clause.generate(SNOWFLAKE_HUSKY_CONTEXT, self.query, TAXON_MODEL_INFO_MAP)
                else:
                    with self.assertRaises(UnknownOperator):
                        filter_clause.generate(SNOWFLAKE_HUSKY_CONTEXT, self.query, TAXON_MODEL_INFO_MAP)

    def test_taxon_taxon_clause_generate(self):
        for operation in SIMPLE_OPERATORS_FUNCTIONS.keys():
            with self.subTest(f'Taxon taxon clause test with operator {operation.value}'):
                actual = TaxonTaxonFilterClause(
                    {
                        'type': FilterClauseType.TAXON_TAXON.value,
                        'taxon': 'test',
                        'right_taxon': 'test_other',
                        'operator': operation.value,
                    }
                ).generate(SNOWFLAKE_HUSKY_CONTEXT, self.query, TAXON_MODEL_INFO_MAP)
                expected = f'path.to.schema.test {operation.value} path.to.schema.test_other'
                self.assertEqual(expected, str(actual))

        for operation in [SimpleFilterOperator.LIKE, SimpleFilterOperator.NOT_LIKE]:
            with self.subTest(f'Taxon taxon clause test with operator {operation.value}'):
                actual = TaxonTaxonFilterClause(
                    {
                        'type': FilterClauseType.TAXON_TAXON.value,
                        'taxon': 'test',
                        'right_taxon': 'test_other',
                        'operator': operation.value,
                    }
                ).generate(SNOWFLAKE_HUSKY_CONTEXT, self.query, TAXON_MODEL_INFO_MAP)
                expected = f"CAST(path.to.schema.test AS VARCHAR) {operation.value.replace('_', ' ')} path.to.schema.test_other ESCAPE '/'"
                self.assertEqual(expected, str(actual))

        for operation in [SimpleFilterOperator.ILIKE, SimpleFilterOperator.NOT_ILIKE]:
            with self.subTest(f'Taxon taxon clause test with operator {operation.value}'):
                actual = TaxonTaxonFilterClause(
                    {
                        'type': FilterClauseType.TAXON_TAXON.value,
                        'taxon': 'test',
                        'right_taxon': 'test_other',
                        'operator': operation.value,
                    }
                ).generate(SNOWFLAKE_HUSKY_CONTEXT, self.query, TAXON_MODEL_INFO_MAP)
                expected = f"lower(CAST(path.to.schema.test AS VARCHAR)) {operation.value.replace('_', ' ').replace('ILIKE', 'LIKE')} lower(path.to.schema.test_other) ESCAPE '/'"
                self.assertEqual(expected, str(actual))

    def test_taxon_array_clause_generate(self):
        with self.subTest('Taxon array clause simple test - single value'):
            actual = TaxonArrayFilterClause(
                {
                    'type': FilterClauseType.TAXON_ARRAY,
                    'taxon': 'test',
                    'operator': ArrayFilterOperator.IN,
                    'value': ['10'],
                }
            ).generate(SNOWFLAKE_HUSKY_CONTEXT, self.query, TAXON_MODEL_INFO_MAP)
            expected = 'path.to.schema.test IN (:param_1)'

            self.assertEqual(expected, str(actual))

        with self.subTest('Taxon array clause simple test - single value & negate'):
            actual = TaxonArrayFilterClause(
                {
                    'type': FilterClauseType.TAXON_ARRAY,
                    'taxon': 'test',
                    'operator': ArrayFilterOperator.NOT_IN,
                    'value': ['10'],
                }
            ).generate(SNOWFLAKE_HUSKY_CONTEXT, self.query, TAXON_MODEL_INFO_MAP)
            expected = 'path.to.schema.test NOT IN (:param_1)'

            self.assertEqual(expected, str(actual))

        with self.subTest('Taxon array clause simple test - multiple values'):
            actual = TaxonArrayFilterClause(
                {
                    'type': FilterClauseType.TAXON_ARRAY,
                    'taxon': 'test',
                    'operator': ArrayFilterOperator.IN,
                    'value': ['10', 20],
                }
            ).generate(SNOWFLAKE_HUSKY_CONTEXT, self.query, TAXON_MODEL_INFO_MAP)
            expected = 'path.to.schema.test IN (:param_1, :param_2)'

            self.assertEqual(expected, str(actual))

    def test_group_clause_supported_operators_generate(self):
        for operator in GROUP_OPERATORS_FUNCTIONS:
            for negate in [True, False]:
                with self.subTest(f'Group clause with operator {operator.value} & negate = {negate}'):
                    actual = GroupFilterClause(
                        {
                            'type': FilterClauseType.GROUP.value,
                            'negate': negate,
                            'logical_operator': operator.value,
                            'clauses': [
                                TaxonValueFilterClause(
                                    {
                                        'type': FilterClauseType.TAXON_VALUE.value,
                                        'taxon': 'test',
                                        'operator': SimpleFilterOperator.EQ.value,
                                        'value': '10',
                                    }
                                ),
                                TaxonValueFilterClause(
                                    {
                                        'type': FilterClauseType.TAXON_VALUE.value,
                                        'taxon': 'test_next',
                                        'operator': SimpleFilterOperator.EQ.value,
                                        'value': '20',
                                    }
                                ),
                            ],
                        }
                    ).generate(SNOWFLAKE_HUSKY_CONTEXT, self.query, TAXON_MODEL_INFO_MAP)

                    expected = f'(path.to.schema.test = :param_1 {operator.value} path.to.schema.test_next = :param_2)'
                    if negate:
                        expected = f'NOT {expected}'

                    self.assertEqual(expected, str(actual))

    def test_group_clause_nested_clauses_generate(self):
        actual = GroupFilterClause(
            {
                'type': FilterClauseType.GROUP.value,
                'logical_operator': LogicalOperator.AND.value,
                'clauses': [
                    GroupFilterClause(
                        {
                            'type': FilterClauseType.GROUP.value,
                            'logical_operator': LogicalOperator.OR.value,
                            'negate': True,
                            'clauses': [
                                TaxonValueFilterClause(
                                    {
                                        'type': FilterClauseType.TAXON_VALUE.value,
                                        'taxon': 'test',
                                        'operator': SimpleFilterOperator.EQ.value,
                                        'value': '10',
                                    }
                                ),
                                TaxonValueFilterClause(
                                    {
                                        'type': FilterClauseType.TAXON_VALUE.value,
                                        'taxon': 'test',
                                        'operator': SimpleFilterOperator.EQ.value,
                                        'value': '20',
                                    }
                                ),
                                TaxonValueFilterClause(
                                    {
                                        'type': FilterClauseType.TAXON_VALUE.value,
                                        'taxon': 'test',
                                        'operator': SimpleFilterOperator.EQ.value,
                                        'value': '30',
                                    }
                                ),
                            ],
                        }
                    ),
                    TaxonValueFilterClause(
                        {
                            'type': FilterClauseType.TAXON_VALUE.value,
                            'taxon': 'test',
                            'operator': SimpleFilterOperator.EQ.value,
                            'value': '40',
                        }
                    ),
                ],
            }
        ).generate(SNOWFLAKE_HUSKY_CONTEXT, self.query, TAXON_MODEL_INFO_MAP)

        first_clause = f'NOT (path.to.schema.test = :param_1 {LogicalOperator.OR.value} path.to.schema.test = :param_2 {LogicalOperator.OR.value} path.to.schema.test = :param_3)'
        second_clause = 'path.to.schema.test = :param_4'

        expected = f'({first_clause} {LogicalOperator.AND.value} {second_clause})'

        self.assertEqual(expected, str(actual))

    def test_taxon_value_caluse_taxon_slugs(self):
        actual = TaxonValueFilterClause(
            {
                'type': FilterClauseType.TAXON_VALUE.value,
                'taxon': 'test',
                'operator': SimpleFilterOperator.LIKE.value,
                'value': '10',
            }
        ).get_taxon_slugs()
        expected = {'test'}

        self.assertSetEqual(expected, actual)

    def test_taxon_taoxn_clause_taxon_slugs(self):
        actual = TaxonTaxonFilterClause(
            {
                'type': FilterClauseType.TAXON_TAXON.value,
                'taxon': 'test_left',
                'right_taxon': 'test_right',
                'operator': SimpleFilterOperator.LIKE.value,
            }
        ).get_taxon_slugs()
        expected = {'test_left', 'test_right'}

        self.assertSetEqual(expected, actual)

    def test_taxon_array_clause_taxon_slugs(self):
        actual = TaxonArrayFilterClause(
            {'type': FilterClauseType.TAXON_ARRAY, 'taxon': 'test', 'operator': ArrayFilterOperator.IN, 'value': ['10']}
        ).get_taxon_slugs()
        expected = {'test'}

        self.assertSetEqual(expected, actual)

    def test_group_clause_nested_clauses_taxon_slugs(self):
        actual = GroupFilterClause(
            {
                'type': FilterClauseType.GROUP.value,
                'logical_operator': LogicalOperator.AND.value,
                'clauses': [
                    GroupFilterClause(
                        {
                            'type': FilterClauseType.GROUP.value,
                            'logical_operator': LogicalOperator.OR.value,
                            'clauses': [
                                TaxonValueFilterClause(
                                    {
                                        'type': FilterClauseType.TAXON_VALUE.value,
                                        'taxon': 'test',
                                        'operator': SimpleFilterOperator.EQ.value,
                                        'value': '10',
                                    }
                                ),
                                TaxonValueFilterClause(
                                    {
                                        'type': FilterClauseType.TAXON_VALUE.value,
                                        'taxon': 'test2',
                                        'operator': SimpleFilterOperator.EQ.value,
                                        'value': '20',
                                    }
                                ),
                                TaxonValueFilterClause(
                                    {
                                        'type': FilterClauseType.TAXON_VALUE.value,
                                        'taxon': 'test3',
                                        'operator': SimpleFilterOperator.EQ.value,
                                        'value': '30',
                                    }
                                ),
                            ],
                        }
                    ),
                    TaxonValueFilterClause(
                        {
                            'type': FilterClauseType.TAXON_VALUE.value,
                            'taxon': 'test',
                            'operator': SimpleFilterOperator.EQ.value,
                            'value': '40',
                        }
                    ),
                ],
            }
        ).get_taxon_slugs()
        expected = {'test', 'test2', 'test3'}

        self.assertSetEqual(expected, actual)

    def test_generate_group_filter_clause_dict(self):
        test_cases = [
            (
                LogicalOperator.AND,
                [
                    {
                        'type': FilterClauseType.TAXON_VALUE.value,
                        'operator': SimpleFilterOperator.EQ.value,
                        'value': 'val',
                        'taxon': 'taxon',
                    }
                ],
            ),
            (
                LogicalOperator.OR,
                [
                    {
                        'type': FilterClauseType.TAXON_VALUE.value,
                        'operator': SimpleFilterOperator.EQ.value,
                        'value': 'val',
                        'taxon': 'taxon',
                    }
                ],
            ),
        ]
        for idx, test_case in enumerate(test_cases):
            with self.subTest(f'Test case {idx}'):
                model = GroupFilterClause(generate_group_filter_clause_dict(test_case[0], test_case[1]))
                model.validate()

    def test_generate_simple_filter_clause_dict(self):
        test_cases = [(SimpleFilterOperator.EQ, 'val', 'taxon'), (SimpleFilterOperator.LT, 19, 'taxon-2')]

        for idx, test_case in enumerate(test_cases):
            with self.subTest(f'Test case {idx}'):
                model = TaxonValueFilterClause(
                    generate_simple_filter_clause_dict(test_case[0], test_case[1], test_case[2])
                )
                model.validate()
