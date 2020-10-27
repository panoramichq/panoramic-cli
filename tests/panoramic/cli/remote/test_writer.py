from unittest.mock import Mock, call, patch, sentinel

import pytest
from requests.exceptions import RequestException

from panoramic.cli.errors import (
    DatasetWriteException,
    FieldWriteException,
    InvalidDatasetException,
    InvalidFieldException,
    InvalidModelException,
    ModelWriteException,
)
from panoramic.cli.field_mapper import map_field_slug_from_local
from panoramic.cli.remote.writer import RemoteWriter


@pytest.fixture
def mock_model_client():
    with patch('panoramic.cli.remote.writer.ModelClient', autospec=True) as client:
        yield client()


@pytest.fixture
def mock_vds_client():
    with patch('panoramic.cli.remote.writer.VirtualDataSourceClient', autospec=True) as client:
        yield client()


@pytest.fixture
def mock_field_client():
    with patch('panoramic.cli.remote.writer.FieldClient', autospec=True) as client:
        yield client()


@patch('panoramic.cli.remote.writer.map_data_source_from_local')
def test_writer_write_data_source_invalid_exception(_, mock_vds_client):
    mock_vds_client.upsert_virtual_data_source.side_effect = RequestException('test', response=Mock(status_code=400))

    with pytest.raises(InvalidDatasetException):
        RemoteWriter('test_company').write_data_source(Mock())


@patch('panoramic.cli.remote.writer.map_data_source_from_local')
def test_writer_write_data_source_exception(_, mock_vds_client):
    mock_vds_client.upsert_virtual_data_source.side_effect = RequestException('test')

    with pytest.raises(DatasetWriteException):
        RemoteWriter('test_company').write_data_source(Mock())


@patch('panoramic.cli.remote.writer.map_data_source_from_local')
def test_writer_write_data_source(mock_map, mock_vds_client):
    mock_map.return_value = sentinel.remote_dataset

    RemoteWriter('test_company').write_data_source(Mock())

    assert mock_vds_client.upsert_virtual_data_source.mock_calls == [call('test_company', sentinel.remote_dataset)]


@patch('panoramic.cli.remote.writer.map_model_from_local')
def test_writer_write_model_invalid_exception(_, mock_model_client):
    mock_model_client.upsert_model.side_effect = RequestException('test', response=Mock(status_code=400))

    with pytest.raises(InvalidModelException):
        RemoteWriter('test_company').write_model(Mock())


@patch('panoramic.cli.remote.writer.map_model_from_local')
def test_writer_write_model_exception(_, mock_model_client):
    mock_model_client.upsert_model.side_effect = RequestException('test')

    with pytest.raises(ModelWriteException):
        RemoteWriter('test_company').write_model(Mock())


@patch('panoramic.cli.remote.writer.map_model_from_local')
def test_writer_model(mock_map, mock_model_client):
    mock_map.return_value = sentinel.remote_model

    RemoteWriter('test_company').write_model(Mock(virtual_data_source='test_dataset'))

    assert mock_model_client.upsert_model.mock_calls == [call('test_dataset', 'test_company', sentinel.remote_model)]


def test_writer_delete_model_exception(mock_model_client):
    model = Mock(model_name='test_model', virtual_data_source='test_dataset')
    mock_model_client.delete_model.side_effect = RequestException('test')

    with pytest.raises(ModelWriteException):
        RemoteWriter('test_company').delete_model(model)

    assert mock_model_client.delete_model.mock_calls == [call('test_dataset', 'test_company', 'test_model')]


def test_writer_delete_data_source_exception(mock_vds_client):
    data_source = Mock(dataset_slug='test_dataset')
    mock_vds_client.delete_virtual_data_source.side_effect = RequestException('test')

    with pytest.raises(DatasetWriteException):
        RemoteWriter('test_company').delete_data_source(data_source)


def test_writer_delete_field_exception(mock_field_client):
    field = Mock(slug='slug', data_source='data_source')

    mock_field_client.delete_fields.side_effect = RequestException('test')

    with pytest.raises(FieldWriteException):
        RemoteWriter('test_company').delete_field(field)


def test_writer_delete_model(mock_model_client):
    model = Mock(model_name='test_model', virtual_data_source='test_dataset')

    RemoteWriter('test_company').delete_model(model)

    assert mock_model_client.delete_model.mock_calls == [call('test_dataset', 'test_company', 'test_model')]


def test_writer_delete_data_source(mock_vds_client):
    data_source = Mock(dataset_slug='test_dataset')

    RemoteWriter('test_company').delete_data_source(data_source)

    assert mock_vds_client.delete_virtual_data_source.mock_calls == [call('test_company', 'test_dataset')]


def test_writer_delete_field(mock_field_client):
    field = Mock(slug='slug', data_source='data_source')
    remote_slug = map_field_slug_from_local('slug', 'data_source')

    RemoteWriter('test_company').delete_field(field)

    mock_field_client.delete_fields.assert_called_with(company_slug='test_company', slugs=[remote_slug])


@patch('panoramic.cli.remote.writer.map_field_from_local')
def test_writer_write_field(mock__map_field_from_local, mock_field_client):
    mock__map_field_from_local.return_value = sentinel.remote_field

    RemoteWriter('test_company').write_field(Mock())

    assert mock_field_client.upsert_fields.mock_calls == [
        call(company_slug='test_company', fields=[sentinel.remote_field])
    ]


@patch('panoramic.cli.remote.writer.map_field_from_local')
def test_writer_write_field_invalid_exception(_, mock_field_client):
    mock_field_client.upsert_fields.side_effect = RequestException('test', response=Mock(status_code=400))

    with pytest.raises(InvalidFieldException):
        RemoteWriter('test_company').write_field(Mock())


@patch('panoramic.cli.remote.writer.map_field_from_local')
def test_writer_write_field_source_exception(_, mock_field_client):
    mock_field_client.upsert_fields.side_effect = RequestException('test')

    with pytest.raises(FieldWriteException):
        RemoteWriter('test_company').write_field(Mock())
