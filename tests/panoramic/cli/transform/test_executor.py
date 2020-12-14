from unittest.mock import call, patch, sentinel

from panoramic.cli.transform.executor import TransformExecutor
from panoramic.cli.transform.pano_transform import CompiledTransform, PanoTransform


@patch('panoramic.cli.transform.executor.Connections.execute', autospec=True)
@patch('panoramic.cli.transform.executor.Connections.get_by_name', autospec=True)
@patch('panoramic.cli.transform.executor.get_dialect_credentials', autospec=True)
def test_executes_on_a_connection(mock_get_dialect_credentials, mock_connections_get_by_name, mock_connections_execute):
    transform = PanoTransform(name='test', fields=['a'], target='connection.schema.view_name')
    compiled_transform = CompiledTransform(transform=transform, company_slug='company_slug', compiled_query='q')

    mock_connections_get_by_name.return_value = {}
    mock_get_dialect_credentials.return_value = (sentinel.credentials, None)

    TransformExecutor.execute(compiled_transform)

    assert mock_connections_execute.mock_calls == [
        call(sql=compiled_transform.compiled_query, credentials=sentinel.credentials),
        call(sql=compiled_transform.correctness_query, credentials=sentinel.credentials),
    ]
