import pytest
import responses

from panoramic.cli.companies.client import CompaniesClient


@pytest.fixture(autouse=True)
def mock_token_url(monkeypatch):
    monkeypatch.setenv('PANORAMIC_AUTH_TOKEN_URL', 'https://token')


@responses.activate
def test_get_companies():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(
        responses.GET,
        'https://diesel/companies',
        json={'data': ['test-company-1', 'test-company-2', 'test-company-1']},
    )

    client = CompaniesClient(base_url='https://diesel/companies', client_id='client-id', client_secret='client-secret')

    assert client.get_companies() == {'test-company-1', 'test-company-2'}
