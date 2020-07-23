from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from panoramic.cli import cli


@pytest.mark.vcr
def test_list_connections_e2e(monkeypatch):
    monkeypatch.chdir(Path('e2e') / 'scenarios' / 'pano-list-connections')
    runner = CliRunner()

    result = runner.invoke(cli, ['list-connections'])

    assert result.exit_code == 0
    assert set(result.stdout.strip().split('\n')) == {'SF', 'all_hands_demo_1_1'}


@patch('panoramic.cli.command.list_connections')
@pytest.mark.vcr
def test_list_connections_error_e2e(mock_list_connections, monkeypatch):
    monkeypatch.chdir(Path('e2e') / 'scenarios' / 'pano-list-connections')
    runner = CliRunner()

    mock_list_connections.side_effect = Exception('Test Exception')

    result = runner.invoke(cli, ['list-connections'])

    assert result.exit_code == 1
    assert result.stdout.startswith('Error: Internal error occurred\nTraceback (most recent call last):\n')
    assert result.stdout.endswith('raise effect\nException: Test Exception\n\n')
