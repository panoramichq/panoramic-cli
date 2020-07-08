import pytest
import responses

from panoramic.cli.virtual_data_source.client import VirtualDataSourceClient


@pytest.fixture(autouse=True)
def mock_token_url(monkeypatch):
    monkeypatch.setenv('PANORAMIC_AUTH_TOKEN_URL', 'https://token')


@responses.activate
def test_create_virtual_data_source():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(responses.POST, 'https://diesel/virtual?company_id=123', json={'display_name': 'test'})

    client = VirtualDataSourceClient(
        base_url='https://diesel/virtual/', client_id='client-id', client_secret='client-secret'
    )
    client.create_virtual_data_source('123', {'display_name': 'test'})


@responses.activate
def test_get_all_virtual_data_sources():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})

    fake_source = {'display_name': 'virtual_source', 'company_id': '50', 'slug': 'made_up_source'}
    responses.add(
        responses.GET, 'https://diesel/virtual?company_id=50', json={'data': [fake_source]},
    )

    client = VirtualDataSourceClient(
        base_url='https://diesel/virtual/', client_id='client-id', client_secret='client-secret'
    )

    assert client.get_all_virtual_data_sources(company_id='50') == [fake_source]


@responses.activate
def test_get_virtual_data_source():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})

    fake_source = {'display_name': 'virtual_source', 'company_id': '50', 'slug': 'made_up_source'}
    responses.add(
        responses.GET, 'https://diesel/virtual/made_up_source?company_id=50', json={'data': fake_source},
    )

    client = VirtualDataSourceClient(
        base_url='https://diesel/virtual/', client_id='client-id', client_secret='client-secret'
    )
    remote_source = client.get_virtual_data_source('50', fake_source['slug'])

    assert remote_source == fake_source


@responses.activate
def test_update_virtual_data_source():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    fake_source = {'display_name': 'virtual_source', 'company_id': '50', 'slug': 'made_up_source'}

    responses.add(
        responses.PUT, 'https://diesel/virtual/made_up_source?company_id=50', json={'display_name': 'different names'},
    )
    client = VirtualDataSourceClient(
        base_url='https://diesel/virtual/', client_id='client-id', client_secret='client-secret'
    )
    client.update_virtual_data_source('50', fake_source['slug'], {'display_name': 'different names'})


@responses.activate
def test_delete_virtual_data_source():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(responses.DELETE, 'https://diesel/virtual/made_up_source?company_id=123')

    client = VirtualDataSourceClient(
        base_url='https://diesel/virtual', client_id='client-id', client_secret='client-secret'
    )
    client.delete_virtual_data_source('123', 'made_up_source')
