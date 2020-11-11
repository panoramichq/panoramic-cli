import pytest

from panoramic.cli.errors import MissingFieldFileError
from panoramic.cli.field.client import Field
from panoramic.cli.field_mapper import (
    map_column_to_field,
    map_error_to_field,
    map_field_from_local,
    map_field_from_remote,
)
from panoramic.cli.pano_model import PanoField


@pytest.fixture()
def dummy_local_field() -> PanoField:
    data = dict(
        slug='slug',
        field_type='field_type',
        group='group',
        display_name='Display name',
        data_type='data_type',
        aggregation={'type': 'aggregation_type', 'params': {'a': 'b'}},
    )
    return PanoField.from_dict(data)


@pytest.fixture()
def dummy_remote_field() -> Field:
    data = dict(
        slug='slug',
        field_type='field_type',
        group='group',
        display_name='Display name',
        data_type='data_type',
        aggregation={'type': 'aggregation_type', 'params': {'a': 'b'}},
    )
    return Field.from_dict(data)


def test_field_local_to_remote(dummy_remote_field, dummy_local_field):
    assert map_field_from_local(dummy_local_field) == dummy_remote_field


def test_field_local_to_remote_data_source_bound(dummy_remote_field, dummy_local_field):
    dummy_remote_field.slug = 'data_source|slug'
    dummy_remote_field.data_source = 'data_source'
    dummy_local_field.data_source = 'data_source'

    assert map_field_from_local(dummy_local_field) == dummy_remote_field


def test_field_remote_to_local(dummy_remote_field, dummy_local_field):
    assert map_field_from_remote(dummy_remote_field) == dummy_local_field


def test_field_remote_to_local_data_source_bound(dummy_remote_field, dummy_local_field):
    dummy_remote_field.slug = 'data_source|slug'
    dummy_remote_field.data_source = 'data_source'
    dummy_local_field.data_source = 'data_source'

    assert map_field_from_remote(dummy_remote_field) == dummy_local_field


def test_map_column_to_field_basic():
    actual = map_column_to_field(
        {
            'column_name': 'slug',
            'data_type': 'TEXT',
            'taxon_type': 'dimension',
            'field_map': ['slug'],
            'data_reference': '"slug"',
            'aggregation_type': None,
            'validation_type': 'text',
        }
    )
    expected = PanoField.from_dict(
        dict(slug='slug', field_type='dimension', group='CLI', display_name='slug', data_type='text')
    )
    assert actual == expected


def test_map_column_to_field_aggregation():
    actual = map_column_to_field(
        {
            'column_name': 'slug',
            'data_type': 'FLOAT',
            'taxon_type': 'metric',
            'field_map': ['slug'],
            'data_reference': '"slug"',
            'aggregation_type': 'sum',
            'validation_type': 'numeric',
        }
    )
    expected = PanoField.from_dict(
        dict(
            slug='slug',
            field_type='metric',
            group='CLI',
            display_name='slug',
            data_type='numeric',
            aggregation=dict(type='sum'),
        )
    )
    assert actual == expected


def test_map_column_to_field_identifier():
    actual = map_column_to_field(
        {
            'column_name': 'slug',
            'data_type': 'TEXT',
            'taxon_type': 'metric',
            'field_map': ['slug'],
            'data_reference': '"slug"',
            'aggregation_type': None,
            'validation_type': 'text',
        },
        is_identifier=True,
    )
    expected = PanoField.from_dict(
        dict(slug='slug', field_type='dimension', group='CLI', display_name='slug', data_type='text')
    )
    assert actual == expected


def test_map_error_to_field():
    assert map_error_to_field(MissingFieldFileError(field_slug='test_field', dataset_slug='test_dataset')) == PanoField(
        slug='test_field',
        display_name='test_field',
        group='CLI',
        data_type='TODO: add data_type',
        field_type='TODO: add field_type',
    )
