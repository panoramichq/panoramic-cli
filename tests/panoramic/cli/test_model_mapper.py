import pytest

from panoramic.cli.model.client import Model, ModelField
from panoramic.cli.model_mapper import (
    map_columns_to_model,
    map_model_from_local,
    map_model_from_remote,
)
from panoramic.cli.pano_model import PanoModel, PanoModelField


@pytest.fixture
def local_model_fixture():
    yield PanoModel(
        model_name='source_schema1_table',
        data_source='source.schema1.table',
        fields=[
            PanoModelField(
                data_reference='id',
                field_map=['id'],
            ),
            PanoModelField(
                data_reference='value',
                field_map=['value'],
            ),
        ],
        joins=[],
        identifiers=[],
    )


@pytest.fixture
def remote_model_fixture():
    yield Model(
        model_name="source_schema1_table",
        data_source="source.schema1.table",
        fields=[
            ModelField(
                field_map=["id"],
                data_reference="id",
            ),
            ModelField(
                field_map=["value"],
                data_reference="value",
            ),
        ],
        joins=[],
        identifiers=[],
        visibility='available',
    )


def test_from_remote_to_local(remote_model_fixture, local_model_fixture):
    expected_local_model = local_model_fixture.to_dict()

    local_model = map_model_from_remote(remote_model_fixture)

    assert local_model.to_dict() == expected_local_model


def test_from_local_to_remote(local_model_fixture, remote_model_fixture):
    expected_remote_model = remote_model_fixture.to_dict()

    remote_model = map_model_from_local(local_model_fixture)

    assert remote_model.to_dict() == expected_remote_model


def test_map_columns_to_model():
    expected = [
        {
            'api_version': 'v1',
            'model_name': 'sourceschema1table1',
            'data_source': 'source.schema1.table1',
            'fields': [
                {
                    'field_map': ['id'],
                    'data_reference': 'id',
                },
                {
                    'field_map': ['value'],
                    'data_reference': 'value',
                },
            ],
            'identifiers': [],
            'joins': [],
        }
    ]
    output = [
        item.to_dict()
        for item in map_columns_to_model(
            [
                {
                    'field_map': ['id'],
                    'data_reference': 'id',
                    'model_name': 'sourceschema1table1',
                    'data_source': 'source.schema1.table1',
                },
                {
                    'field_map': ['value'],
                    'data_reference': 'value',
                    'model_name': 'sourceschema1table1',
                    'data_source': 'source.schema1.table1',
                },
            ],
        )
    ]

    assert output == expected
