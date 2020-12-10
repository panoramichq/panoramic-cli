from unittest.mock import patch

import pytest

from panoramic.cli.transform.executor import TransformExecutor
from panoramic.cli.transform.pano_transform import PanoTransform


@pytest.fixture
def mock_transform_client():
    with patch('panoramic.cli.transform.executor.TransformClient', autospec=True) as client:
        yield client()


def test_wraps_with_create_view_statement(mock_transform_client):
    mock_transform_client.compile_transform.return_value = 'SELECT 1'
    transform = PanoTransform(name='test', fields=['a'], target='connection.schema.view_name')

    transform_executor = TransformExecutor.from_transform(company_slug='company_slug', transform=transform)
    assert transform_executor.compiled_query == 'CREATE OR REPLACE VIEW schema.view_name AS (SELECT 1)'
    mock_transform_client.compile_transform.assert_called_with(transform, 'company_slug', 'connection')
