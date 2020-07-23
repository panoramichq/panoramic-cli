from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from panoramic.cli import cli


@pytest.mark.vcr
def test_push_e2e(monkeypatch):
    monkeypatch.chdir(Path('e2e') / 'scenarios' / 'pano-push')

    runner = CliRunner()
    result = runner.invoke(cli, ['push'])

    assert result.exit_code == 0


@pytest.mark.vcr
@patch('panoramic.cli.command.push')
def test_push_error_e2e(mock_push, monkeypatch):
    monkeypatch.chdir(Path('e2e') / 'scenarios' / 'pano-push')
    mock_push.side_effect = Exception('Test Exception')

    runner = CliRunner()
    result = runner.invoke(cli, ['push'])

    assert result.exit_code == 1
    assert result.stdout.startswith('Error: Internal error occurred\nTraceback (most recent call last):\n')
    assert result.stdout.endswith('raise effect\nException: Test Exception\n\n')
