from unittest.mock import ANY, call, patch

import pytest

from panoramic.cli.command import scan


@pytest.fixture
def mock_refresher():
    with patch('panoramic.cli.command.Refresher') as mock_scanner:
        yield mock_scanner()


@pytest.fixture
def mock_scanner():
    with patch('panoramic.cli.command.Scanner') as mock_scanner:
        yield mock_scanner()


@pytest.fixture
def mock_writer():
    with patch('panoramic.cli.command.FileWriter') as mock_writer:
        yield mock_writer()


def test_scan(mock_writer, mock_scanner, mock_refresher):
    mock_scanner.scan_tables.return_value = [{'table_schema': 'source.schema1', 'table_name': 'table1'}]
    mock_scanner.scan_columns.return_value = [
        {'table_schema': 'source.schema1', 'table_name': 'table1', 'column_name': 'id', 'data_type': 'str'},
        {'table_schema': 'source.schema1', 'table_name': 'table1', 'column_name': 'value', 'data_type': 'int'},
    ]

    scan('test-source', 'test-filter')

    assert mock_refresher.refresh_table.mock_calls == [call('schema1.table1')]
    assert mock_writer.write_model.mock_calls == [call(ANY)]


def test_scan_single_table_error(mock_writer, mock_scanner, mock_refresher):
    mock_scanner.scan_tables.return_value = [{'table_schema': 'source.schema1', 'table_name': 'table1'}]
    mock_scanner.scan_columns.return_value = [
        {'table_schema': 'source.schema1', 'table_name': 'table1', 'column_name': 'id', 'data_type': 'str'},
        {'table_schema': 'source.schema1', 'table_name': 'table1', 'column_name': 'value', 'data_type': 'int'},
    ]
    mock_refresher.refresh_table.side_effect == [Exception('test'), None]

    scan('test-source', 'test-filter')

    assert mock_writer.write_model.mock_calls == [call(ANY)]
