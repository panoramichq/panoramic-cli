import pytest
import responses

from panoramic.cli.virtual_data_source.client import (
    VirtualDataSource,
    VirtualDataSourceClient,
)


@pytest.fixture(autouse=True)
def mock_token_url(monkeypatch):
    monkeypatch.setenv('PANORAMIC_AUTH_TOKEN_URL', 'https://token')


@responses.activate
def test_upsert_virtual_data_source():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(
        responses.PUT,
        'https://diesel/virtual?company_slug=test-company',
        json={'display_name': 'test', 'slug': 'bug'},
    )

    client = VirtualDataSourceClient(
        base_url='https://diesel/virtual/', client_id='client-id', client_secret='client-secret'
    )
    client.upsert_virtual_data_source('test-company', VirtualDataSource(display_name='test', slug='bug'))


@responses.activate
def test_get_all_virtual_data_sources():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})

    fake_source = VirtualDataSource(display_name='virtual_source', slug='test-source')
    responses.add(
        responses.GET,
        'https://diesel/virtual?company_slug=test-company&offset=200&limit=100',
        json={'data': [fake_source.to_dict()]},
    )

    client = VirtualDataSourceClient(
        base_url='https://diesel/virtual/', client_id='client-id', client_secret='client-secret'
    )

    assert client.get_all_virtual_data_sources('test-company', offset=200, limit=100) == [fake_source]


@responses.activate
def test_get_virtual_data_source():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})

    fake_source = VirtualDataSource(display_name='virtual_source', slug='test-source')
    responses.add(
        responses.GET,
        'https://diesel/virtual/test-source?company_slug=test-company',
        json={'data': fake_source.to_dict()},
    )

    client = VirtualDataSourceClient(
        base_url='https://diesel/virtual/', client_id='client-id', client_secret='client-secret'
    )
    remote_source = client.get_virtual_data_source('test-company', fake_source.slug)

    assert remote_source == fake_source


@responses.activate
def test_delete_virtual_data_source():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(responses.DELETE, 'https://diesel/virtual/test-source?company_slug=test-company')

    client = VirtualDataSourceClient(
        base_url='https://diesel/virtual/', client_id='client-id', client_secret='client-secret'
    )
    client.delete_virtual_data_source('test-company', 'test-source')
