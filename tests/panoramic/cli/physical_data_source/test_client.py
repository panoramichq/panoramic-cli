import pytest
import responses

from panoramic.cli.physical_data_source.client import PhysicalDataSourceClient


@pytest.fixture(autouse=True)
def mock_token_url(monkeypatch):
    monkeypatch.setenv('PANORAMIC_AUTH_TOKEN_URL', 'https://token')


@responses.activate
def test_get_sources():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(
        responses.GET,
        'https://diesel/source/?company_slug=company_name',
        json={
            'data': [
                # ... sth should be here
            ]
        },
    )

    client = PhysicalDataSourceClient(
        base_url='https://diesel/source/', client_id='client-id', client_secret='client-secret'
    )
    client.get_sources('company_name')

    assert len(responses.calls) == 2
