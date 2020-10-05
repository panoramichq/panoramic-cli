import pytest
import responses

from panoramic.cli.join.client import TERMINAL_STATES, JobState, JoinClient


@pytest.fixture(autouse=True)
def mock_token_url(monkeypatch):
    monkeypatch.setenv('PANORAMIC_AUTH_TOKEN_URL', 'https://token')


@responses.activate
def test_create_join_detection_job():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(
        responses.POST,
        'https://joins/test-dataset?company_slug=test-company',
        json={'data': {'job_id': 'test-job-id'}},
    )

    client = JoinClient(base_url='https://joins/', client_id='client-id', client_secret='client-secret')

    assert client.create_join_detection_job('test-company', 'test-dataset') == 'test-job-id'


@responses.activate
def test_get_job_status():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(
        responses.GET, 'https://joins/job/test-job-id?company_slug=test-company', json={'data': {'status': 'RUNNING'}}
    )

    client = JoinClient(base_url='https://joins/', client_id='client-id', client_secret='client-secret')

    assert client.get_job_status(company_slug='test-company', job_id='test-job-id') == JobState.RUNNING


#
@responses.activate
def test_get_job_results():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(
        responses.GET,
        'https://joins/job/test-job-id?company_slug=test-company',
        json={'data': {'job_id': 'test-job-id', 'status': 'COMPLETED', 'joins': {}}},
    )

    client = JoinClient(base_url='https://joins/', client_id='client-id', client_secret='client-secret')

    assert client.get_job_results(company_slug='test-company', job_id='test-job-id') == {
        'job_id': 'test-job-id',
        'status': 'COMPLETED',
        'joins': {},
    }


@responses.activate
@pytest.mark.parametrize('final_state', TERMINAL_STATES)
def test_wait_for_terminal_state(final_state):
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(
        responses.GET, 'https://joins/job/test-job-id?company_slug=test-company', json={'data': {'status': 'RUNNING'}}
    )
    responses.add(
        responses.GET,
        'https://joins/job/test-job-id?company_slug=test-company',
        json={'data': {'status': final_state.value}},
    )

    client = JoinClient(base_url='https://joins/', client_id='client-id', client_secret='client-secret')

    assert client.wait_for_terminal_state(company_slug='test-company', job_id='test-job-id') == final_state
