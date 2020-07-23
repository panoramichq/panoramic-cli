from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

from panoramic.cli import cli
from panoramic.cli.local.file_utils import Paths


@pytest.mark.vcr
def test_init_e2e(monkeypatch, tmpdir):
    monkeypatch.chdir(str(tmpdir))
    runner = CliRunner()

    result = runner.invoke(cli, ['init'], input='test-company')

    print(result.stdout)
    assert result.exit_code == 0
    with Paths.context_file().open() as f:
        assert yaml.safe_load(f.read()) == {'company_slug': 'test-company', 'api_version': 'v1'}


@pytest.mark.vcr
@patch('panoramic.cli.command.initialize')
def test_init_error_e2e(mock_init, monkeypatch, tmpdir):
    monkeypatch.chdir(str(tmpdir))
    runner = CliRunner()

    mock_init.side_effect = Exception('Test Exception')

    result = runner.invoke(cli, ['init'])

    assert result.exit_code == 1
    assert result.stdout.startswith('Error: Internal error occurred\nTraceback (most recent call last):\n')
    assert result.stdout.endswith('raise effect\nException: Test Exception\n\n')
