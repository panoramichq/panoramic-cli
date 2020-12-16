from unittest.mock import call, patch, sentinel

import pytest

from panoramic.cli.errors import ConnectionFormatException, ConnectionNotFound
from panoramic.cli.transform.executor import TransformExecutor
from panoramic.cli.transform.pano_transform import CompiledTransform, PanoTransform


@patch('panoramic.cli.transform.executor.Connections.execute')
@patch('panoramic.cli.transform.executor.Connections.get_by_name')
@patch('panoramic.cli.transform.executor.get_dialect_credentials')
def test_executes_on_a_connection(mock_get_dialect_credentials, mock_connections_get_by_name, mock_connections_execute):
    transform = PanoTransform(name='test', fields=['a'], target='connection.schema.view_name')
    compiled_transform = CompiledTransform(transform=transform, company_slug='company_slug', compiled_query='q')

    mock_connections_get_by_name.return_value = {}
    mock_get_dialect_credentials.return_value = (sentinel.credentials, None)

    TransformExecutor.execute(compiled_transform)

    mock_connections_get_by_name.assert_called_with('connection')
    assert mock_connections_execute.mock_calls == [
        call(sql=compiled_transform.compiled_query, credentials=sentinel.credentials),
        call(sql=compiled_transform.correctness_query, credentials=sentinel.credentials),
    ]


@patch('panoramic.cli.transform.executor.Connections.execute')
@patch('panoramic.cli.transform.executor.Connections.get_by_name')
@patch('panoramic.cli.transform.executor.get_dialect_credentials')
def test_execute_fails_with_malformed_credentials(
    mock_get_dialect_credentials, mock_connections_get_by_name, mock_connections_execute
):
    transform = PanoTransform(name='test', fields=['a'], target='connection.schema.view_name')
    compiled_transform = CompiledTransform(transform=transform, company_slug='company_slug', compiled_query='q')

    mock_connections_get_by_name.return_value = {}
    mock_get_dialect_credentials.return_value = (None, 'An error occured')

    with pytest.raises(ConnectionFormatException):
        TransformExecutor.execute(compiled_transform)

    mock_connections_get_by_name.assert_called_with('connection')
    assert mock_connections_execute.call_count == 0


@patch('panoramic.cli.transform.executor.Connections.execute')
@patch('panoramic.cli.transform.executor.Connections.get_by_name')
@patch('panoramic.cli.transform.executor.get_dialect_credentials')
def test_execute_fails_without_connection(
    mock_get_dialect_credentials, mock_connections_get_by_name, mock_connections_execute
):
    transform = PanoTransform(name='test', fields=['a'], target='connection.schema.view_name')
    compiled_transform = CompiledTransform(transform=transform, company_slug='company_slug', compiled_query='q')

    mock_connections_get_by_name.side_effect = ValueError('connection')

    with pytest.raises(ConnectionNotFound):
        TransformExecutor.execute(compiled_transform)

    assert mock_get_dialect_credentials.call_count == 0
    assert mock_connections_execute.call_count == 0
