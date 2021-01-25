from unittest.mock import call, patch

import pytest

from panoramic.cli.errors import ConnectionNotFound
from panoramic.cli.transform.executor import TransformExecutor
from panoramic.cli.transform.pano_transform import CompiledTransform, PanoTransform


@patch('panoramic.cli.transform.executor.Connections.execute')
@patch('panoramic.cli.transform.executor.Connections.get_by_name')
@patch('sqlalchemy.engine.create_engine')
def test_executes_on_a_connection(mock_create_engine, mock_connections_get_by_name, mock_connections_execute):
    transform = PanoTransform(name='test', fields=['a'], target='connection.schema.view_name')
    compiled_transform = CompiledTransform(transform=transform, company_id='company_id', compiled_query='q')

    mock_connections_get_by_name.return_value = {}

    TransformExecutor.execute(compiled_transform)

    mock_connections_get_by_name.assert_called_with('connection')
    assert mock_connections_execute.mock_calls == [
        call(sql=compiled_transform.compiled_query, connection={}),
        call(sql=compiled_transform.correctness_query, connection={}),
    ]


@patch('panoramic.cli.transform.executor.Connections.execute')
@patch('panoramic.cli.transform.executor.Connections.get_by_name')
@patch('sqlalchemy.engine.create_engine')
def test_execute_fails_without_connection(mock_create_engine, mock_connections_get_by_name, mock_connections_execute):
    transform = PanoTransform(name='test', fields=['a'], target='connection.schema.view_name')
    compiled_transform = CompiledTransform(transform=transform, company_id='company_id', compiled_query='q')

    mock_connections_get_by_name.side_effect = ValueError('connection')

    with pytest.raises(ConnectionNotFound):
        TransformExecutor.execute(compiled_transform)

    assert mock_create_engine.call_count == 0
    assert mock_connections_execute.call_count == 0
