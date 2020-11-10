import pytest
import responses

from panoramic.cli.model.client import Model, ModelClient


@pytest.fixture(autouse=True)
def mock_token_url(monkeypatch):
    monkeypatch.setenv('PANORAMIC_AUTH_TOKEN_URL', 'https://token')


@responses.activate
def test_delete_model():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(responses.DELETE, 'https://diesel/model?virtual_data_source=test-source&company_slug=test-company')

    client = ModelClient(base_url='https://diesel/', client_id='client-id', client_secret='client-secret')

    client.delete_model('test-source', 'test-company', 'model')

    assert len(responses.calls) == 2


@responses.activate
def test_get_models():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    model = Model(
        model_name='model', data_source="db.schema.table", fields=[], joins=[], identifiers=[], visibility='available'
    )
    responses.add(responses.GET, 'https://diesel/', json={'data': [model.to_dict()]})

    client = ModelClient(base_url='https://diesel/', client_id='client-id', client_secret='client-secret')

    assert client.get_models('test-source', 'test-company') == [model]


@responses.activate
def test_get_model_names():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(responses.GET, 'https://diesel/model-name', json={'data': ['a', 'b']})

    client = ModelClient(base_url='https://diesel/', client_id='client-id', client_secret='client-secret')

    assert client.get_model_names('test-source', 'test-company') == ['a', 'b']


@responses.activate
def test_upsert_model():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(
        responses.PUT,
        'https://diesel/?virtual_data_source=test-source&company_slug=test-company',
        json={},
    )

    client = ModelClient(base_url='https://diesel/', client_id='client-id', client_secret='client-secret')

    client.upsert_model(
        'test-source',
        'test-company',
        Model(
            model_name='model',
            data_source="db.schema.table",
            fields=[],
            joins=[],
            identifiers=[],
            visibility='available',
        ),
    )

    assert len(responses.calls) == 2
