import pytest
from pydantic import ValidationError

from panoramic.cli.husky.core.enums import DbDataType
from panoramic.cli.husky.core.federated.model.models import (
    FdqModel,
    FdqModelJoinRelationship,
)
from panoramic.cli.husky.core.model.enums import JoinType, ModelVisibility


def _tmp_model(fields=None, joins=None, identifiers=None):
    return {
        'model_name': 'model-name',
        'data_source': 'fqn.model_1',
        'fields': fields if fields is not None else [],
        'joins': joins if joins is not None else [],
        'identifiers': identifiers if identifiers is not None else [],
        'visibility': ModelVisibility.available.value,
    }


@pytest.mark.parametrize(
    'model_definition,expected_errors',
    [
        # test case 1
        (
            _tmp_model(
                [
                    {'data_reference': '"column_name"', 'field_map': ['taxon_1'], 'data_type': DbDataType.INTEGER},
                    {
                        'data_reference': '"column_name" + 10',
                        'field_map': ['taxon_1'],
                        'data_type': DbDataType.INTEGER,
                    },
                ]
            ),
            ['exc=ValueError(\'Following fields are mapped more than once - taxon_1\') loc=(\'__root__\',)'],
        ),
        # test case 2
        (
            _tmp_model(
                [
                    {'data_reference': '"column_name"', 'field_map': ['taxon_1'], 'data_type': DbDataType.INTEGER},
                    {
                        'data_reference': '"column_name" + taxon_3',
                        'field_map': ['taxon_2'],
                        'data_type': DbDataType.INTEGER,
                    },
                ]
            ),
            [
                (
                    "exc=ValidationError(model='FdqModel', errors=[{'loc': ('attributes',), "
                    '\'msg\': \'Data reference ""column_name" + taxon_3": taxon_3 not available '
                    "in this model', 'type': 'value_error'}]) loc=('__root__',)"
                )
            ],
        ),
        # test case 3
        (
            _tmp_model(
                [
                    {'data_reference': '"column_name"', 'field_map': ['taxon_1'], 'data_type': DbDataType.INTEGER},
                    {
                        'data_reference': '"column_name" + taxon_2',
                        'field_map': ['taxon_2'],
                        'data_type': DbDataType.INTEGER,
                    },
                ]
            ),
            [
                (
                    'exc=ValueError(\'Data reference ""column_name" + taxon_2" contains TEL transformation '
                    'with cyclic reference\') loc=(\'__root__\',)'
                )
            ],
        ),
        # test case 4
        (
            _tmp_model(
                [
                    {'data_reference': '"column_name"', 'field_map': ['taxon_1'], 'data_type': DbDataType.INTEGER},
                    {
                        'data_reference': '"column_name_2" + taxon_3',
                        'field_map': ['taxon_2'],
                        'data_type': DbDataType.INTEGER,
                    },
                    {
                        'data_reference': '\"column_name_3\" + 5 / taxon_2',
                        'field_map': ['taxon_3'],
                        'data_type': DbDataType.INTEGER,
                    },
                ]
            ),
            [
                (
                    'exc=ValueError(\'Data reference ""column_name_2" + taxon_3" contains '
                    'TEL transformation with cyclic reference\') loc=(\'__root__\',)'
                )
            ],
        ),
        # test case 5
        (
            _tmp_model(
                [
                    {
                        'data_reference': '"column_name_3" + 5 / taxon_3',
                        'field_map': ['taxon_3'],
                        'data_type': DbDataType.INTEGER,
                    }
                ]
            ),
            [
                (
                    'exc=ValueError(\'Data reference ""column_name_3" + 5 / taxon_3" contains '
                    'TEL transformation with cyclic reference\') loc=(\'__root__\',)'
                )
            ],
        ),
        # test case 6
        (
            _tmp_model(
                [
                    {'data_reference': '"column_name"', 'field_map': ['taxon_2'], 'data_type': DbDataType.INTEGER},
                    {
                        'data_reference': '"column_name_2" + taxon_2',
                        'field_map': ['taxon_3'],
                        'data_type': DbDataType.DATE,
                    },
                    {
                        'data_reference': '"column_name_3" + 5',
                        'field_map': ['taxon_4'],
                        'data_type': DbDataType.DATE,
                    },
                ],
                [
                    {
                        'join_type': JoinType.left,
                        'to_model': 'model_b',
                        'relationship': FdqModelJoinRelationship.one_to_one,
                        'fields': ['taxon_5', 'taxon_4'],
                    }
                ],
                ['taxon_3', 'taxon_4'],
            ),
            [
                "exc=ValidationError(model='FdqModel', errors=[{'loc': ('joins',), 'msg': 'Join 1 contains missing fields taxon_5', 'type': 'value_error'}]) loc=('__root__',)"
            ],
        ),
        # test case 7
        (
            _tmp_model(
                [
                    {'data_reference': '"column_name"', 'field_map': ['taxon_2'], 'data_type': DbDataType.INTEGER},
                    {
                        'data_reference': '"column_name_2" + taxon_2',
                        'field_map': ['taxon_3', 'taxon_6'],
                        'data_type': DbDataType.INTEGER,
                    },
                    {'data_reference': '"column_name_3"', 'field_map': ['taxon_4'], 'data_type': DbDataType.DATE},
                    {'data_reference': '"column_name_3"', 'field_map': ['taxon_5'], 'data_type': DbDataType.DATE},
                ],
                [
                    {
                        'join_type': JoinType.left,
                        'to_model': 'model_b',
                        'relationship': FdqModelJoinRelationship.one_to_one,
                        'fields': ['taxon_4', 'taxon_5'],
                    }
                ],
                ['taxon_4', 'taxon_5'],
            ),
            [],
        ),
        # test case 8
        (
            _tmp_model(
                [
                    {'data_reference': '"column_name"', 'field_map': ['taxon_1'], 'data_type': DbDataType.INTEGER},
                    {
                        'data_reference': '"column_name_2" + taxon_3',
                        'field_map': ['taxon_x', 'taxon_5'],
                        'data_type': DbDataType.INTEGER,
                    },
                    {
                        'data_reference': '"column_name_3" + 5 / taxon_5',
                        'field_map': ['taxon_3'],
                        'data_type': DbDataType.INTEGER,
                    },
                ]
            ),
            [
                (
                    'exc=ValueError(\'Data reference ""column_name_2" + taxon_3" contains '
                    'TEL transformation with cyclic reference\') loc=(\'__root__\',)'
                )
            ],
        ),
        # identifiers not present as attributes
        (
            _tmp_model(
                fields=[{'data_reference': '"column_name"', 'field_map': ['taxon_1'], 'data_type': DbDataType.INTEGER}],
                identifiers=['taxon_2'],
            ),
            [("exc=ValueError('Identifier(s) taxon_2 are not present as fields on the model') loc=('__root__',)")],
        ),
    ],
)
def test_model_validation(model_definition, expected_errors):
    if len(expected_errors):
        # we do expect validation errors
        with pytest.raises(ValidationError) as exception_info:
            FdqModel.parse_obj(model_definition)

        # sort pydantic error messages
        actual_errors = sorted([str(err) for err in exception_info.value.args[0]])
        assert actual_errors == expected_errors
    else:
        # we dont expect any problems
        FdqModel.parse_obj(model_definition)
