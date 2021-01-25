from unittest.mock import Mock, patch

import pytest

from panoramic.cli.command import delete_orphaned_fields
from panoramic.cli.errors import OrphanFieldFileError
from panoramic.cli.local.executor import LocalExecutor


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


@pytest.fixture()
def mock_physical_data_source_client():
    with patch('panoramic.cli.command.PhysicalDataSourceClient') as client_class:
        yield client_class()


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


# @patch('panoramic.cli.command.get_local_state')
# @patch.object(LocalExecutor, '_execute')
# def test_scaffold_missing_files(mock_execute, mock_state, capsys):
#     mock_state.return_value.get_objects_by_package.return_value.items.return_value = [
#         (
#             'test_dataset',
#             (
#                 [],
#                 [Mock(data_source='db.schema.test_table', identifiers=['id'], fields=[Mock(field_map=['test_slug'])])],
#             ),
#         ),
#     ]
#
#     scaffold_missing_fields(yes=True)
#
#     assert mock_execute.mock_calls == [call(Action(desired=sentinel.field))]
#     assert capsys.readouterr().out == (
#         "Loading local state...\n\n"
#         "Fields referenced in models without definition in dataset test_dataset:\n"
#         "  test_slug\n\n"
#         "Scanning fields...\n"
#         "Updating local state...\n"
#         "Updated 1/1 fields\n"
#     )
