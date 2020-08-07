from unittest.mock import Mock, patch, sentinel

import pytest
from requests.exceptions import HTTPError

from panoramic.cli.errors import ScanException, SourceNotFoundException
from panoramic.cli.metadata.client import TERMINAL_STATES, JobState
from panoramic.cli.scan import Scanner


@patch('panoramic.cli.scan.MetadataClient')
def test_scan_columns_completed(mock_client):
    mock_client.return_value.create_get_columns_job.return_value = 'test-job-id'
    mock_client.return_value.wait_for_terminal_state.return_value = JobState.COMPLETED
    mock_client.return_value.collect_results.return_value = [sentinel.value1, sentinel.value2]

    assert list(Scanner('test-company', 'test-source').scan_columns(table_filter='table-name')) == [
        sentinel.value1,
        sentinel.value2,
    ]


@pytest.mark.parametrize('final_state', TERMINAL_STATES - {JobState.COMPLETED})
@patch('panoramic.cli.scan.MetadataClient')
def test_scan_columns_non_completed(mock_client, final_state):
    mock_client.return_value.create_get_columns_job.return_value = 'test-job-id'
    mock_client.return_value.wait_for_terminal_state.return_value = final_state

    # TODO: add proper exception
    with pytest.raises(Exception):
        list(Scanner('test-company', 'test-source').scan_columns(table_filter='table-name'))


@patch('panoramic.cli.scan.MetadataClient')
def test_scan_columns_not_found(mock_client):
    mock_client.return_value.create_get_columns_job.side_effect = HTTPError(response=Mock(status_code=404))

    with pytest.raises(SourceNotFoundException):
        list(Scanner('test-company', 'test-source').scan_columns(table_filter='table-name', timeout=1))


@patch('panoramic.cli.scan.MetadataClient')
def test_scan_columns_generic_error(mock_client):
    mock_client.return_value.create_get_columns_job.side_effect = HTTPError(response=Mock(status_code=500))

    with pytest.raises(ScanException):
        list(Scanner('test-company', 'test-source').scan_columns(table_filter='table-name', timeout=1))


@patch('panoramic.cli.scan.MetadataClient')
def test_scan_tables_completed(mock_client):
    mock_client.return_value.create_get_tables_job.return_value = 'test-job-id'
    mock_client.return_value.wait_for_terminal_state.return_value = JobState.COMPLETED
    mock_client.return_value.collect_results.return_value = [sentinel.value1, sentinel.value2]

    assert list(Scanner('test-company', 'test-source').scan_tables(table_filter='table-name')) == [
        sentinel.value1,
        sentinel.value2,
    ]


@pytest.mark.parametrize('final_state', TERMINAL_STATES - {JobState.COMPLETED})
@patch('panoramic.cli.scan.MetadataClient')
def test_scan_tables_non_completed(mock_client, final_state):
    mock_client.return_value.create_get_tables_job.return_value = 'test-job-id'
    mock_client.return_value.get_job_status.return_value = final_state

    # TODO: add proper exception
    with pytest.raises(Exception):
        list(Scanner('test-company', 'test-source').scan_tables(table_filter='table-name'))


@patch('panoramic.cli.scan.MetadataClient')
def test_scan_tables_not_found(mock_client):
    mock_client.return_value.create_get_tables_job.side_effect = HTTPError(response=Mock(status_code=404))

    with pytest.raises(SourceNotFoundException):
        list(Scanner('test-company', 'test-source').scan_tables(table_filter='table-name', timeout=1))


@patch('panoramic.cli.scan.MetadataClient')
def test_scan_tables_generic_error(mock_client):
    mock_client.return_value.create_get_tables_job.side_effect = HTTPError(response=Mock(status_code=500))

    with pytest.raises(ScanException):
        list(Scanner('test-company', 'test-source').scan_tables(table_filter='table-name', timeout=1))
