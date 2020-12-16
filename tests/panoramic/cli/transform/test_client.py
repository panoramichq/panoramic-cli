import pytest
import responses

from panoramic.cli.transform.client import TransformClient
from panoramic.cli.transform.pano_transform import PanoTransform


@pytest.fixture(autouse=True)
def mock_token_url(monkeypatch):
    monkeypatch.setenv('PANORAMIC_AUTH_TOKEN_URL', 'https://token')


@responses.activate
def test_compile():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(
        responses.POST,
        'https://transform/compile?company_slug=test-company&physical_data_source=connection',
        json={'data': {'sql': 'SELECT something from tables'}},
    )

    client = TransformClient(base_url='https://transform/', client_id='client-id', client_secret='client-secret')
    transform = PanoTransform(name='test_transform', fields=['field', 'another_field'], target='connection.schema')

    client.compile_transform(transform, 'test-company', 'connection')

    assert len(responses.calls) == 2
