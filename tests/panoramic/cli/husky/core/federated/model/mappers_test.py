import pytest

from panoramic.cli.husky.core.enums import DbDataType
from panoramic.cli.husky.core.federated.model.mappers import (
    FdqModelAttributeMapper,
    FdqModelJoinMapper,
    FdqModelMapper,
)
from panoramic.cli.husky.core.federated.model.models import (
    FdqModel,
    FdqModelAttribute,
    FdqModelJoin,
    FdqModelJoinRelationship,
)
from panoramic.cli.husky.core.federated.utils import (
    prefix_with_virtual_data_source,
    remove_virtual_data_source_prefix,
)
from panoramic.cli.husky.core.model.enums import (
    JoinType,
    ModelVisibility,
    Relationship,
    ValueQuantityType,
)
from panoramic.cli.husky.core.model.models import HuskyModel, ModelAttribute, ModelJoin

_VIRTUAL_DATA_SOURCE = 'virtual_source_slug'
_COMPANY_ID = '123'
_USER_ID = 'usr-id'


@pytest.mark.parametrize(
    ['transformation', 'model_attrs'],
    [
        (
            '"another_col"',
            [
                ModelAttribute(
                    {
                        'column_sql_type': DbDataType.CHARACTER_VARYING,
                        'tel_transformation': '"another_col"',
                        'taxon': f'{_VIRTUAL_DATA_SOURCE}|taxon_slug',
                    }
                )
            ],
        ),
        (
            '"col_name"',
            [
                ModelAttribute(
                    {
                        'column_sql_type': DbDataType.SMALLINT,
                        'tel_transformation': '"col_name"',
                        'taxon': f'{_VIRTUAL_DATA_SOURCE}|taxon_slug_2',
                    }
                )
            ],
        ),
    ],
)
def test_api_model_attribute_from_internal(transformation, model_attrs):
    attr = FdqModelAttributeMapper.from_internal(transformation, model_attrs, _VIRTUAL_DATA_SOURCE)

    assert attr.dict(by_alias=True) == {
        'data_type': model_attrs[0].column_sql_type,
        'data_reference': transformation,
        'field_map': [remove_virtual_data_source_prefix(_VIRTUAL_DATA_SOURCE, model_attrs[0].taxon)],
    }


@pytest.mark.parametrize(
    'model_attr',
    [
        FdqModelAttribute(data_type=DbDataType.DATE, data_reference='"first_col"', field_map=['taxon_slug']),
        FdqModelAttribute(data_type=DbDataType.INTEGER, data_reference='"col_name"', field_map=['taxon_slug_2']),
    ],
)
def test_api_model_attribute_to_internal(model_attr):
    identifiers = {'taxon_slug'}
    attrs = FdqModelAttributeMapper.to_internal(model_attr, _VIRTUAL_DATA_SOURCE, identifiers)

    assert [attr.to_primitive() for attr in attrs] == [
        {
            'column_sql_type': model_attr.data_type,
            'tel_transformation': model_attr.data_reference,
            'taxon': prefix_with_virtual_data_source(_VIRTUAL_DATA_SOURCE, model_attr.field_map[0]),
            'identifier': model_attr.field_map[0] in identifiers,
            'quantity_type': ValueQuantityType.scalar.value,
        }
    ]


def test_api_model_join_to_internal():
    api_join = FdqModelJoin(
        join_type=JoinType.left,
        to_model='model_1',
        fields=['taxon_slug', 'taxon_slug_2'],
        relationship=FdqModelJoinRelationship.many_to_one,
    )
    model_join = FdqModelJoinMapper.to_internal(api_join, _VIRTUAL_DATA_SOURCE)

    assert model_join.to_primitive() == {
        'direction': None,
        'join_type': api_join.join_type.value,
        'to_model': prefix_with_virtual_data_source(_VIRTUAL_DATA_SOURCE, api_join.to_model),
        'relationship': api_join.relationship.value,
        'taxons': [prefix_with_virtual_data_source(_VIRTUAL_DATA_SOURCE, taxon) for taxon in api_join.taxons],
    }


def test_api_model_join_from_internal():
    model_join = ModelJoin(
        {
            'join_type': JoinType.left,
            'to_model': prefix_with_virtual_data_source(_VIRTUAL_DATA_SOURCE, 'model_1'),
            'taxons': [
                prefix_with_virtual_data_source(_VIRTUAL_DATA_SOURCE, 'taxon_slug'),
                prefix_with_virtual_data_source(_VIRTUAL_DATA_SOURCE, 'taxon_slug_2'),
            ],
            'relationship': Relationship.many_to_one,
        }
    )
    api_join = FdqModelJoinMapper.from_internal(model_join, _VIRTUAL_DATA_SOURCE)

    assert api_join.dict(by_alias=True) == {
        'join_type': api_join.join_type,
        'to_model': remove_virtual_data_source_prefix(_VIRTUAL_DATA_SOURCE, model_join.to_model),
        'relationship': FdqModelJoinRelationship(api_join.relationship.value),
        'fields': [remove_virtual_data_source_prefix(_VIRTUAL_DATA_SOURCE, taxon) for taxon in model_join.taxons],
    }


@pytest.mark.parametrize(
    'api_model',
    [
        FdqModel(
            model_name='api-model-slug',
            data_source='physical.db.schema.table',
            fields=[],
            joins=[],
            visibility=ModelVisibility.hidden,
        ),
        FdqModel(
            model_name='api-model-slug-2',
            data_source='physical.table2',
            fields=[
                FdqModelAttribute(
                    data_type=DbDataType.INTEGER, data_reference='"col_name"', field_map=['taxon_slug_3']
                ),
                FdqModelAttribute(
                    data_type=DbDataType.DATE,
                    data_reference='"col_name_2"',
                    field_map=['taxon_slug', 'taxon_slug_2'],
                ),
            ],
            identifiers=['taxon_slug', 'taxon_slug_2'],
            joins=[
                FdqModelJoin(
                    join_type=JoinType.left,
                    to_model='model_1',
                    fields=['taxon_slug', 'taxon_slug_2'],
                    relationship=FdqModelJoinRelationship.many_to_one,
                )
            ],
            visibility=ModelVisibility.hidden,
        ),
    ],
)
def test_api_model_to_internal(api_model):
    husky_model = FdqModelMapper.to_internal(api_model, _VIRTUAL_DATA_SOURCE, 'company_id')
    assert husky_model.to_primitive() == {
        'data_sources': [_VIRTUAL_DATA_SOURCE],
        'fully_qualified_name_parts': api_model.data_source.split('.'),
        'model_type': 'metric',
        'time_granularity': None,
        'attributes': {
            attr.taxon: attr.to_primitive()
            for api_attr in api_model.attributes
            for attr in FdqModelAttributeMapper.to_internal(api_attr, _VIRTUAL_DATA_SOURCE, set(api_model.identifiers))
        },
        'company_id': 'company_id',
        'joins': [
            FdqModelJoinMapper.to_internal(join, _VIRTUAL_DATA_SOURCE).to_primitive() for join in api_model.joins
        ],
        'name': prefix_with_virtual_data_source(_VIRTUAL_DATA_SOURCE, api_model.model_name),
        'project_id': None,
        'visibility': api_model.visibility.name,
    }


@pytest.mark.parametrize(
    'husky_model',
    [
        HuskyModel(
            {
                'data_sources': [_VIRTUAL_DATA_SOURCE],
                'fully_qualified_name_parts': ['physical', 'db', 'schema', 'table'],
                'model_type': 'metric',
                'attributes': {},
                'company_id': _COMPANY_ID,
                'joins': [],
                'name': prefix_with_virtual_data_source(_VIRTUAL_DATA_SOURCE, 'husky_name_slug'),
                'visibility': ModelVisibility.hidden,
            }
        ),
        HuskyModel(
            {
                'name': prefix_with_virtual_data_source(_VIRTUAL_DATA_SOURCE, 'api-model-slug-2'),
                'data_sources': [_VIRTUAL_DATA_SOURCE],
                'fully_qualified_name_parts': ['physical', 'db', 'table2'],
                'company_id': _COMPANY_ID,
                'attributes': {
                    prefix_with_virtual_data_source(_VIRTUAL_DATA_SOURCE, 'taxon_slug'): {
                        'column_sql_type': DbDataType.DATE,
                        'tel_transformation': '"col_name"',
                        'taxon': prefix_with_virtual_data_source(_VIRTUAL_DATA_SOURCE, 'taxon_slug'),
                        'identifier': True,
                    },
                    prefix_with_virtual_data_source(_VIRTUAL_DATA_SOURCE, 'taxon_slug_2'): {
                        'tel_transformation': '"another_co"',
                        'column_sql_type': DbDataType.INTEGER,
                        'taxon': prefix_with_virtual_data_source(_VIRTUAL_DATA_SOURCE, 'taxon_slug_2'),
                        'identifier': False,
                    },
                },
                'joins': [
                    {
                        'direction': None,
                        'join_type': JoinType.left.name,
                        'to_model': prefix_with_virtual_data_source(_VIRTUAL_DATA_SOURCE, 'another_model'),
                        'relationship': Relationship.many_to_one.value,
                        'taxons': [prefix_with_virtual_data_source(_VIRTUAL_DATA_SOURCE, 'taxon_slug')],
                    }
                ],
                'visibility': ModelVisibility.available.name,
            }
        ),
    ],
)
def test_api_model_from_internal(husky_model):
    api_model = FdqModelMapper.from_internal(husky_model)
    assert api_model.dict(by_alias=True) == {
        'data_source': '.'.join(husky_model.fully_qualified_name_parts),
        'fields': [
            FdqModelAttributeMapper.from_internal(attr.tel_transformation, [attr], _VIRTUAL_DATA_SOURCE).dict()
            for attr in husky_model.attributes.values()
        ],
        'identifiers': [
            remove_virtual_data_source_prefix(_VIRTUAL_DATA_SOURCE, slug)
            for slug, a in husky_model.attributes.items()
            if a.identifier
        ],
        'joins': [
            FdqModelJoinMapper.from_internal(join, _VIRTUAL_DATA_SOURCE).dict(by_alias=True)
            for join in husky_model.joins
        ],
        'model_name': remove_virtual_data_source_prefix(_VIRTUAL_DATA_SOURCE, husky_model.name),
        'visibility': husky_model.visibility,
    }
