from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from panoramic.cli import cli


@pytest.mark.vcr
def test_scan_e2e(monkeypatch):
    monkeypatch.chdir(Path('e2e') / 'scenarios' / 'pano-scan')
    runner = CliRunner()

    result = runner.invoke(cli, ['scan', 'SF', '--filter', 'METRICS3_STG.ADWORDS_VIEWS.ENTITY%'])

    assert result.exit_code == 0


@pytest.mark.vcr
@patch('panoramic.cli.command.scan')
def test_scan_error_e2e(mock_scan, monkeypatch):
    monkeypatch.chdir(Path('e2e') / 'scenarios' / 'pano-scan')
    mock_scan.side_effect = Exception('Test Exception')
    runner = CliRunner()

    result = runner.invoke(cli, ['scan', 'SF', '--filter', 'METRICS3_STG.ADWORDS_VIEWS.ENTITY%'])

    assert result.exit_code == 1
    assert (
        result.stdout
        == """Error: Internal error occurred\nTraceback (most recent call last):\n  File "/Users/marknemec/code/pano-cli/src/panoramic/cli/cli.py", line 41, in scan\n    scan_command(source_id, filter, parallel)\n  File "/Users/marknemec/.pyenv/versions/3.8.2/lib/python3.8/unittest/mock.py", line 1081, in __call__\n    return self._mock_call(*args, **kwargs)\n  File "/Users/marknemec/.pyenv/versions/3.8.2/lib/python3.8/unittest/mock.py", line 1085, in _mock_call\n    return self._execute_mock_call(*args, **kwargs)\n  File "/Users/marknemec/.pyenv/versions/3.8.2/lib/python3.8/unittest/mock.py", line 1140, in _execute_mock_call\n    raise effect\nException: Test Exception\n\n"""
    )
