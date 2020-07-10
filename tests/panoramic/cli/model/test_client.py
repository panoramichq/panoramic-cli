import pytest
import responses

from panoramic.cli.model.client import ModelClient


@pytest.fixture(autouse=True)
def mock_token_url(monkeypatch):
    monkeypatch.setenv('PANORAMIC_AUTH_TOKEN_URL', 'https://token')


@pytest.mark.skip
@responses.activate
def test_delete_model():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(responses.DELETE, 'https://diesel/model?virtual_data_source=test-source&company_id=test-company')

    client = ModelClient(base_url='https://diesel/', client_id='client-id', client_secret='client-secret')

    client.delete_model('test-source', 'test-company', 'model')

    assert len(responses.calls) == 2


@pytest.mark.skip
@responses.activate
def test_get_models():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(responses.GET, 'https://diesel/', json={'data': [{'a': 'b'}]})

    client = ModelClient(base_url='https://diesel/', client_id='client-id', client_secret='client-secret')

    assert client.get_models('test-source', 'test-company') == [{'a': 'b'}]


@pytest.mark.skip
@responses.activate
def test_get_model_names():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(responses.GET, 'https://diesel/model-name', json={'data': ['a', 'b']})

    client = ModelClient(base_url='https://diesel/', client_id='client-id', client_secret='client-secret')

    assert client.get_model_names('test-source', 'test-company') == ['a', 'b']


@pytest.mark.skip
@responses.activate
def test_upsert_model():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(responses.PUT, 'https://diesel/?virtual_data_source=test-source&company_id=test-company', json={})

    client = ModelClient(base_url='https://diesel/', client_id='client-id', client_secret='client-secret')

    client.upsert_model('test-source', 'test-company', {'name': 'model'})

    assert len(responses.calls) == 2
