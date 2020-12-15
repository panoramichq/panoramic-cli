from unittest.mock import patch

import pytest

from panoramic.cli.transform.compiler import TransformCompiler
from panoramic.cli.transform.pano_transform import PanoTransform


@pytest.fixture
def mock_transform_client():
    with patch('panoramic.cli.transform.compiler.TransformClient', autospec=True) as client:
        yield client()


def test_wraps_with_create_view_statement(mock_transform_client):
    mock_transform_client.compile_transform.return_value = 'SELECT 1'
    transform = PanoTransform(name='test', fields=['a'], target='connection.schema.view_name')

    transform_compiler = TransformCompiler(company_slug="company_slug")
    compiled_transform = transform_compiler.compile(transform=transform)

    assert compiled_transform.company_slug == 'company_slug'
    assert compiled_transform.compiled_query == 'CREATE OR REPLACE VIEW schema.view_name AS (SELECT 1)'
    mock_transform_client.compile_transform.assert_called_with(transform, 'company_slug', 'connection')
