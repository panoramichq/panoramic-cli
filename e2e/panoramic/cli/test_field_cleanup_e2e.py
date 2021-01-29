from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from panoramic.cli import cli
from panoramic.cli.paths import Paths

TEST_CALCULATED_FIELD = """
api_version: v1
slug: calculated_test_field
display_name: Dataset test field
group: Custom
calculation: 10
field_type: metric
data_type: text
"""

TEST_ORPHANED_FIELD = """
api_version: v1
slug: orphan_test_field
display_name: Dataset test field
group: Custom
field_type: metric
data_type: text
"""


@pytest.fixture
def create_fields(monkeypatch):
    monkeypatch.chdir(Path('e2e') / 'scenarios' / 'pano-field-cleanup')
    (Paths.fields_dir(Path('test_dataset')) / 'orphan_test_field.field.yaml').write_text(TEST_ORPHANED_FIELD)
    (Paths.fields_dir(Path('test_dataset')) / 'calculated_test_field.field.yaml').write_text(TEST_CALCULATED_FIELD)
    yield


def test_field_cleanup_e2e(create_fields):
    runner = CliRunner()
    result = runner.invoke(cli, ['field', 'cleanup', '-y'])

    fields_dir = Paths.fields_dir(Path('test_dataset'))

    assert result.exit_code == 0
    assert {f.name for f in fields_dir.iterdir()} == {
        'dataset_test_field.field.yaml',
        'calculated_test_field.field.yaml',
    }
    assert {f.name for f in Paths.company_fields_dir().iterdir()} == {'company_test_field.field.yaml'}


@patch('panoramic.cli.command.delete_orphaned_fields')
def test_field_cleanup_error_e2e(mock_delete, monkeypatch):
    monkeypatch.chdir(Path('e2e') / 'scenarios' / 'pano-push-pull')
    mock_delete.side_effect = Exception('Test Exception')

    runner = CliRunner()
    result = runner.invoke(cli, ['field', 'cleanup'])

    assert result.exit_code == 1
    assert result.stdout.startswith('Error: Internal error occurred\nTraceback (most recent call last):\n')
    assert result.stdout.endswith('raise effect\nException: Test Exception\n\n')
