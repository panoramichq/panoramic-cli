from unittest.mock import Mock, call, patch, sentinel

import pytest
from requests.exceptions import HTTPError

from panoramic.cli.errors import (
    MissingFieldFileError,
    ScanException,
    SourceNotFoundException,
)
from panoramic.cli.metadata.client import TERMINAL_STATES, JobState
from panoramic.cli.scan import Scanner, scan_fields_for_errors


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


@patch('panoramic.cli.scan.get_company_slug', return_value='test-company')
@patch('panoramic.cli.scan.map_error_to_field')
@patch('panoramic.cli.scan.map_column_to_field')
@patch('panoramic.cli.scan.Scanner')
def test_scan_fields_for_errors(
    mock_scanner,
    mock_map_column_to_field,
    mock_map_error_to_field,
    _,
):
    errors = [
        # scanned
        MissingFieldFileError(
            field_slug='test_slug_1',
            dataset_slug='test_dataset',
            data_source='db1.schema.table1',
            data_reference='"TEST_COLUMN"',
            identifier=False,
        ),
        # not scanned
        MissingFieldFileError(
            field_slug='test_slug_2',
            dataset_slug='test_dataset',
            data_source='db2.schema.table1',
            data_reference='10 + 5',
            identifier=False,
        ),
    ]

    mock_scanner_db1 = Mock()
    mock_scanner_db1.scan_columns.return_value = [
        {'data_reference': '"TEST_COLUMN"'},
        {'data_reference': '"SOME_OTHER_COLUMN"'},
    ]
    mock_scanner_db2 = Mock()
    mock_scanner_db2.scan_columns.return_value = [{'data_reference': '"SOME_OTHER_COLUMN"'}]

    mock_scanner.side_effect = lambda _, conn: mock_scanner_db1 if conn == 'db1' else mock_scanner_db2

    mock_map_column_to_field.return_value = sentinel.scanned_field
    mock_map_error_to_field.return_value = sentinel.non_scanned_field

    fields = scan_fields_for_errors(errors)

    assert mock_scanner_db2.scan_columns.mock_calls == [call(table_filter='schema.table1')]
    assert mock_scanner_db1.scan_columns.mock_calls == [call(table_filter='schema.table1')]
    assert set(fields) == {sentinel.scanned_field, sentinel.non_scanned_field}
