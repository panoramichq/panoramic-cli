import pytest

from panoramic.cli.field.client import Field
from panoramic.cli.field_mapper import map_field_from_local, map_field_from_remote
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
