import pytest
import responses

from panoramic.cli.virtual_data_source.client import VirtualDataSourceClient


@pytest.fixture(autouse=True)
def mock_token_url(monkeypatch):
    monkeypatch.setenv('PANORAMIC_AUTH_TOKEN_URL', 'https://token')


@responses.activate
def test_get_all_data_sources():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})

    fake_source = {'display_name': 'virtual_source', 'company_id': '50', 'slug': 'made_up_source'}
    responses.add(
        responses.GET, 'https://diesel/virtual?company_id=50', json={'data': [fake_source]},
    )

    client = VirtualDataSourceClient(
        company_id='50', base_url='https://diesel/virtual/', client_id='client-id', client_secret='client-secret'
    )

    assert client.all() == [fake_source]


@responses.activate
def test_get_data_source():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})

    fake_source = {'display_name': 'virtual_source', 'company_id': '50', 'slug': 'made_up_source'}
    responses.add(
        responses.GET, 'https://diesel/virtual/made_up_source?company_id=50', json={'data': fake_source},
    )

    client = VirtualDataSourceClient(
        company_id='50', base_url='https://diesel/virtual/', client_id='client-id', client_secret='client-secret'
    )
    remote_source = client.get(fake_source['slug'])

    assert remote_source == fake_source


@responses.activate
def test_delete_data_source():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(responses.DELETE, 'https://diesel/virtual/made_up_source?company_id=123')

    client = VirtualDataSourceClient(
        company_id='123', base_url='https://diesel/virtual', client_id='client-id', client_secret='client-secret'
    )
    client.delete('made_up_source')
