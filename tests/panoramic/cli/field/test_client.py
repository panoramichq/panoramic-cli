import pytest
import responses

from panoramic.cli.field.client import Field, FieldClient


@pytest.fixture(autouse=True)
def mock_token_url(monkeypatch):
    monkeypatch.setenv('PANORAMIC_AUTH_TOKEN_URL', 'https://token')


def _create_field(**kwargs) -> Field:
    base = dict(
        slug='some_field', field_type='Namespaced', display_name='Some field', group='group', data_type='data_type'
    )
    base.update(kwargs)

    return Field.from_dict(base)


@responses.activate
def test_upsert_field():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    field = _create_field()
    client = FieldClient(base_url='https://diesel/field/', client_id='client-id', client_secret='client-secret')

    # Create with a data source
    responses.add(
        responses.POST,
        'https://diesel/field/?company_slug=test-company',
        json=[field.to_dict()],
    )
    client.upsert_fields(company_slug='test-company', fields=[field])
    assert len(responses.calls) == 2


@responses.activate
def test_get_fields_dataset_scoped():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    field = _create_field()

    responses.add(
        responses.GET,
        'https://diesel/field/?company_slug=test-company&virtual_data_source=test-source&offset=0&limit=100',
        json={'data': [field.to_dict()]},
    )

    client = FieldClient(base_url='https://diesel/field/', client_id='client-id', client_secret='client-secret')

    assert client.get_fields(data_source='test-source', company_slug='test-company') == [field]
    assert len(responses.calls) == 2


@responses.activate
def test_get_all_fields():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    field = _create_field()

    responses.add(
        responses.GET,
        'https://diesel/field/?company_slug=test-company&offset=0&limit=100',
        json={'data': [field.to_dict()]},
    )

    client = FieldClient(base_url='https://diesel/field/', client_id='client-id', client_secret='client-secret')

    assert client.get_fields(company_slug='test-company') == [field]
    assert len(responses.calls) == 2


@responses.activate
def test_delete_field():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(
        responses.POST,
        'https://diesel/field/delete?company_slug=test-company',
        json=['field', 'vds|another_field'],
    )

    client = FieldClient(
        base_url='https://diesel/field/',
        client_id='client-id',
        client_secret='client-secret',
    )

    client.delete_fields(company_slug='test-company', slugs=['field', 'vds|another_field'])
    assert len(responses.calls) == 2
