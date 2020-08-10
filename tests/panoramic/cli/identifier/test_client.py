import pytest
import responses

from panoramic.cli.identifier.client import TERMINAL_STATES, IdentifierClient, JobState


@pytest.fixture(autouse=True)
def mock_token_url(monkeypatch):
    monkeypatch.setenv('PANORAMIC_AUTH_TOKEN_URL', 'https://token')


@responses.activate
def test_create_identifier_job():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(
        responses.POST,
        'https://diesel/test-source?company_slug=test-company&table_name=test-table',
        json={'data': {'job_id': 'test-job-id'}},
    )

    client = IdentifierClient(base_url='https://diesel/', client_id='client-id', client_secret='client-secret')

    assert client.create_identifier_job('test-company', 'test-source', 'test-table') == 'test-job-id'


@responses.activate
def test_get_job_status():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(responses.GET, 'https://diesel/job/test-job-id', json={'data': {'status': 'RUNNING'}})

    client = IdentifierClient(base_url='https://diesel/', client_id='client-id', client_secret='client-secret')

    assert client.get_job_status('test-job-id') == JobState.RUNNING


@responses.activate
def test_get_job_results():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(responses.GET, 'https://diesel/job/test-job-id', json={'data': ['a', 'b']})

    client = IdentifierClient(base_url='https://diesel/', client_id='client-id', client_secret='client-secret')

    assert client.get_job_results('test-job-id') == ['a', 'b']


@responses.activate
@pytest.mark.parametrize('final_state', TERMINAL_STATES)
def test_wait_for_terminal_state(final_state):
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(responses.GET, 'https://diesel/job/test-job-id', json={'data': {'status': 'RUNNING'}})
    responses.add(responses.GET, 'https://diesel/job/test-job-id', json={'data': {'status': final_state.value}})

    client = IdentifierClient(base_url='https://diesel/', client_id='client-id', client_secret='client-secret')

    assert client.wait_for_terminal_state('test-job-id') == final_state
