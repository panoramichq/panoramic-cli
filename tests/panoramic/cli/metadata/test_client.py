import responses

from panoramic.cli.metadata.client import JobState, MetadataClient


@responses.activate
def test_create_get_columns_job():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(
        responses.POST, 'https://diesel/test-source/columns?table-filter=test-filter', json={'job_id': 'test-job-id'}
    )

    client = MetadataClient(
        base_url='https://diesel/', token_url='https://token', client_id='client-id', client_secret='client-secret'
    )

    assert client.create_get_columns_job('test-source', 'test-filter') == 'test-job-id'


@responses.activate
def test_create_get_tables_job():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(
        responses.POST, 'https://diesel/test-source/tables?table-filter=test-filter', json={'job_id': 'test-job-id'}
    )

    client = MetadataClient(
        base_url='https://diesel/', token_url='https://token', client_id='client-id', client_secret='client-secret'
    )

    assert client.create_get_tables_job('test-source', 'test-filter') == 'test-job-id'


@responses.activate
def test_create_refresh_job():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(
        responses.POST, 'https://diesel/test-source/refresh?table-name=test-table', json={'job_id': 'test-job-id'}
    )

    client = MetadataClient(
        base_url='https://diesel/', token_url='https://token', client_id='client-id', client_secret='client-secret'
    )

    assert client.create_refresh_job('test-source', 'test-table') == 'test-job-id'


@responses.activate
def test_get_job_status():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(responses.GET, 'https://diesel/job/test-job-id', json={'job_status': 'RUNNING'})

    client = MetadataClient(
        base_url='https://diesel/', token_url='https://token', client_id='client-id', client_secret='client-secret'
    )

    assert client.get_job_status('test-job-id') == JobState.RUNNING


@responses.activate
def test_get_job_results():
    responses.add(responses.POST, 'https://token/', json={'access_token': '123123'})
    responses.add(responses.GET, 'https://diesel/job/test-job-id/results', json={'data': [{'a': 'b'}]})

    client = MetadataClient(
        base_url='https://diesel/', token_url='https://token', client_id='client-id', client_secret='client-secret'
    )

    assert client.get_job_results('test-job-id') == [{'a': 'b'}]
