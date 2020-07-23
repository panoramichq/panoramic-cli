from unittest.mock import patch

import pytest
from click.testing import CliRunner

from panoramic.cli import cli


@pytest.mark.vcr
def test_list_companies_e2e():
    runner = CliRunner()

    result = runner.invoke(cli, ['list-companies'])

    assert result.exit_code == 0
    assert set(result.stdout.strip().split('\n')) == {'westeros-knights', 'operam'}


@patch('panoramic.cli.command.list_companies')
@pytest.mark.vcr
def test_list_companies_error_e2e(mock_list_companies):
    runner = CliRunner()

    mock_list_companies.side_effect = Exception('Test Exception')

    result = runner.invoke(cli, ['list-companies'])

    assert result.exit_code == 1
    assert result.stdout.startswith('Error: Internal error occurred\nTraceback (most recent call last):\n')
    assert result.stdout.endswith('raise effect\nException: Test Exception\n\n')
