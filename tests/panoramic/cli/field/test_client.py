import pytest
import responses

from panoramic.cli.field.client import Field, FieldClient


@pytest.fixture(autouse=True)
def mock_token_url(monkeypatch):
    monkeypatch.setenv('PANORAMIC_AUTH_TOKEN_URL', 'https://token')


@responses.activate
def test_create_model():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    field = Field(slug='some_field', field_type='Namespaced', display_name='Some field')
    client = FieldClient(base_url='https://diesel/taxon/', client_id='client-id', client_secret='client-secret')

    # Create with a data source
    responses.add(
        responses.POST, 'https://diesel/taxon/?company_slug=test-company', json=field.to_dict(),
    )
    client.create_field(company_slug='test-company', field=field)
    assert len(responses.calls) == 2


@responses.activate
def test_get_fields():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    field = Field(slug='some_field', field_type='Namespaced', display_name='Some field')

    responses.add(
        responses.GET,
        'https://diesel/taxon/?company_slug=test-company&virtual_data_source=test-source&offset=0&limit=100',
        json={'data': [field.to_dict()]},
    )

    client = FieldClient(base_url='https://diesel/taxon/', client_id='client-id', client_secret='client-secret')

    assert client.get_fields(data_source='test-source', company_slug='test-company') == [field]
    assert len(responses.calls) == 2


@responses.activate
def test_update_fields():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    field = Field(slug='some_field', field_type='Namespaced', display_name='Some field')
    field2 = Field(slug='another_field', field_type='Namespaced', display_name='Another field')

    responses.add(
        responses.PUT,
        'https://diesel/taxon/?company_slug=test-company',
        json={'data': [field.to_dict(), field2.to_dict()]},
    )

    client = FieldClient(base_url='https://diesel/taxon/', client_id='client-id', client_secret='client-secret')
    client.update_fields(company_slug='test-company', fields=[field, field2])


@responses.activate
def test_delete_field():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(  # Delete with a vds
        responses.DELETE, 'https://diesel/taxon/field?virtual_data_source=test-source&company_slug=test-company'
    )
    responses.add(  # Delete without a vds
        responses.DELETE, 'https://diesel/taxon/company-field?company_slug=test-company'
    )

    client = FieldClient(base_url='https://diesel/taxon/', client_id='client-id', client_secret='client-secret')

    client.delete_field(company_slug='test-company', field_slug='field', data_source='test-source')
    client.delete_field(company_slug='test-company', field_slug='company-field')
    assert len(responses.calls) == 3
