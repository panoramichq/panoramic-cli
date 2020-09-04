import pytest
import responses

from panoramic.cli.metadata.client import TERMINAL_STATES, JobState, MetadataClient


@pytest.fixture(autouse=True)
def mock_token_url(monkeypatch):
    monkeypatch.setenv('PANORAMIC_AUTH_TOKEN_URL', 'https://token')


@responses.activate
def test_create_get_columns_job():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(
        responses.POST,
        'https://diesel/test-source/columns?company_slug=test-company&table_filter=test-filter',
        json={'data': {'job_id': 'test-job-id'}},
    )

    client = MetadataClient(base_url='https://diesel/', client_id='client-id', client_secret='client-secret')

    assert client.create_get_columns_job('test-company', 'test-source', 'test-filter') == 'test-job-id'


@responses.activate
def test_create_get_tables_job():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(
        responses.POST,
        'https://diesel/test-source/tables?company_slug=test-company&table_filter=test-filter',
        json={'data': {'job_id': 'test-job-id'}},
    )

    client = MetadataClient(base_url='https://diesel/', client_id='client-id', client_secret='client-secret')

    assert client.create_get_tables_job('test-company', 'test-source', 'test-filter') == 'test-job-id'


@responses.activate
def test_create_refresh_job():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(
        responses.POST,
        'https://diesel/test-source/refresh?company_slug=test-company&table_name=test-table',
        json={'data': {'job_id': 'test-job-id'}},
    )

    client = MetadataClient(base_url='https://diesel/', client_id='client-id', client_secret='client-secret')

    assert client.create_refresh_job('test-company', 'test-source', 'test-table') == 'test-job-id'


@responses.activate
def test_get_job_status():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(responses.GET, 'https://diesel/job/test-job-id', json={'data': {'job_status': 'RUNNING'}})

    client = MetadataClient(base_url='https://diesel/', client_id='client-id', client_secret='client-secret')

    assert client.get_job_status('test-job-id') == JobState.RUNNING


@responses.activate
def test_get_job_results():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(responses.GET, 'https://diesel/job/test-job-id/results', json={'data': [{'a': 'b'}]})

    client = MetadataClient(base_url='https://diesel/', client_id='client-id', client_secret='client-secret')

    assert client.get_job_results('test-job-id') == [{'a': 'b'}]


@responses.activate
@pytest.mark.parametrize('final_state', TERMINAL_STATES)
def test_wait_for_terminal_state(final_state):
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(responses.GET, 'https://diesel/job/test-job-id', json={'data': {'job_status': 'RUNNING'}})
    responses.add(responses.GET, 'https://diesel/job/test-job-id', json={'data': {'job_status': final_state.value}})

    client = MetadataClient(base_url='https://diesel/', client_id='client-id', client_secret='client-secret')

    assert client.wait_for_terminal_state('test-job-id') == final_state


@responses.activate
def test_collect_results():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(responses.GET, 'https://diesel/job/test-job-id/results?offset=0&limit=1', json={'data': [{'a': 'b'}]})
    responses.add(responses.GET, 'https://diesel/job/test-job-id/results?offset=1&limit=1', json={'data': [{'c': 'd'}]})
    responses.add(responses.GET, 'https://diesel/job/test-job-id/results?offset=2&limit=1', json={'data': []})

    client = MetadataClient(base_url='https://diesel/', client_id='client-id', client_secret='client-secret')

    assert list(client.collect_results('test-job-id', limit=1)) == [{'a': 'b'}, {'c': 'd'}]
