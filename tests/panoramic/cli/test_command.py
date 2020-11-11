from unittest.mock import Mock, call, patch

import pytest

from panoramic.cli.command import (
    delete_orphaned_fields,
    list_connections,
    push,
    scaffold_missing_fields,
    scan,
)
from panoramic.cli.errors import (
    InvalidDatasetException,
    InvalidModelException,
    MissingFieldFileError,
    OrphanFieldFileError,
)
from panoramic.cli.local.executor import LocalExecutor
from panoramic.cli.remote.executor import RemoteExecutor


@pytest.fixture(autouse=True)
def mock_get_company_slug():
    with patch('panoramic.cli.command.get_company_slug', return_value='test-company') as mock_get_company_slug:
        yield mock_get_company_slug


@pytest.fixture
def mock_refresher():
    with patch('panoramic.cli.command.Refresher') as mock_refresher:
        yield mock_refresher()


@pytest.fixture
def mock_scanner():
    with patch('panoramic.cli.command.Scanner') as mock_scanner:
        yield mock_scanner()


@pytest.fixture
def mock_id_generator():
    with patch('panoramic.cli.command.IdentifierGenerator') as mock_id_generator:
        yield mock_id_generator()


@pytest.fixture
def mock_writer():
    with patch('panoramic.cli.command.FileWriter', autospec=True) as mock_writer:
        yield mock_writer()


def test_scan(mock_writer, mock_scanner, mock_refresher, mock_id_generator):
    mock_scanner.scan_tables.return_value = [
        {'data_source': 'source.schema1.table1', 'model_name': 'source.schema1.table1'}
    ]
    mock_id_generator.generate.return_value = ['id']
    mock_scanner.scan_columns.return_value = [
        {
            'data_source': 'source.schema1.table1',
            'model_name': 'source.schema1.table1',
            'taxon_type': 'dimension',
            'data_reference': '"ID"',
            'field_map': ['id'],
            'validation_type': 'text',
            'data_type': 'CHARACTER VARYING',
        }
    ]

    scan('test-source', 'test-filter')

    assert mock_refresher.refresh_table.mock_calls == [call('schema1.table1')]
    assert mock_writer.write_scanned_model.call_count == 1
    assert mock_writer.write_scanned_field.call_count == 1


def test_scan_single_table_error(mock_writer, mock_scanner, mock_refresher, mock_id_generator):
    mock_scanner.scan_tables.return_value = [
        {'data_source': 'source.schema1.table1', 'model_name': 'source.schema1.table1'},
        {'data_source': 'source.schema1.table2', 'model_name': 'source.schema1.table2'},
    ]
    mock_id_generator.generate.return_value = ['id']
    mock_scanner.scan_columns.return_value = [
        {
            'data_source': 'source.schema1.table2',
            'model_name': 'source.schema1.table2',
            'taxon_type': 'dimension',
            'data_reference': '"ID"',
            'field_map': ['id'],
            'validation_type': 'text',
            'data_type': 'CHARACTER VARYING',
        }
    ]
    mock_refresher.refresh_table.side_effect = [Exception('test'), None]

    scan('test-source', 'test-filter')

    assert mock_writer.write_scanned_model.call_count == 1
    assert mock_writer.write_scanned_field.call_count == 1


@pytest.fixture()
def mock_physical_data_source_client():
    with patch('panoramic.cli.command.PhysicalDataSourceClient') as client_class:
        yield client_class()


def test_list_connections(mock_physical_data_source_client):
    list_connections()
    mock_physical_data_source_client.get_sources.assert_called_with('test-company')


@patch('panoramic.cli.command.echo_diff')
@patch('panoramic.cli.command.get_company_slug')
@patch('panoramic.cli.command.get_remote_state')
@patch('panoramic.cli.command.get_local_state')
@patch('panoramic.cli.command.click')
@patch('panoramic.cli.command.reconcile')
@patch.object(RemoteExecutor, '_execute')
def test_push_single_error(mock_execute, mock_reconcile, mock_click, _, __, ___, ____, capsys):
    mock_reconcile.return_value = Mock(
        actions=[Mock(description='test-description-1'), Mock(description='test-description-2'), Mock()]
    )
    mock_execute.side_effect = [
        InvalidDatasetException(Mock()),
        InvalidModelException(Mock()),
        None,
    ]

    mock_click.confirm.return_value = True  # Mock user confirming prompt

    push()

    assert mock_execute.call_count == 3
    assert capsys.readouterr().out == (
        "Loading local state...\n"
        "Fetching remote state...\n"
        "Resolving state...\n"
        "\n"
        "Error: Failed to execute action test-description-1:\n"
        "  Invalid dataset submitted\n"
        "Error: Failed to execute action test-description-2:\n"
        "  Invalid model submitted\n"
        "Updated 1/3 models, fields and datasets\n"
    )


@patch('panoramic.cli.command.get_local_state')
@patch('panoramic.cli.command.validate_orphaned_files')
@patch.object(LocalExecutor, '_execute')
def test_delete_orphaned_fields(mock_execute, mock_validate, mock_state, capsys):
    mock_state.return_value.get_objects_by_package.return_value.items.return_value = [
        ('test_dataset', ([Mock(slug='test_slug', data_source='test_dataset')], [Mock()])),
    ]

    mock_validate.return_value = [OrphanFieldFileError(field_slug='test_slug', dataset_slug='test_dataset')]

    delete_orphaned_fields(yes=True)

    assert mock_execute.call_count == 1
    assert capsys.readouterr().out == (
        "Loading local state...\n\n"
        "Fields without calculation or reference in a model in dataset test_dataset:\n"
        "  test_slug\n\n"
        "Updating local state...\n"
        "Updated 1/1 fields\n"
    )


@patch('panoramic.cli.command.get_local_state')
@patch('panoramic.cli.command.validate_missing_files')
@patch.object(LocalExecutor, '_execute')
def test_scaffold_missing_files(mock_execute, mock_validate, mock_state, capsys):
    mock_state.return_value.get_objects_by_package.return_value.items.return_value = [
        ('test_dataset', ([], [Mock(fields=[Mock(field_map=['test_slug'])])])),
    ]

    mock_validate.return_value = [MissingFieldFileError(field_slug='test_slug', dataset_slug='test_dataset')]

    scaffold_missing_fields(yes=True)

    assert mock_execute.call_count == 1
    assert capsys.readouterr().out == (
        "Loading local state...\n\n"
        "Fields referenced in models without definition in dataset test_dataset:\n"
        "  test_slug\n\n"
        "Updating local state...\n"
        "Updated 1/1 fields\n"
    )
