from pathlib import Path

import pytest
from click.testing import CliRunner

from panoramic.cli import cli


@pytest.mark.vcr
def test_scan_e2e(monkeypatch, tmpdir):
    monkeypatch.chdir(Path('e2e') / 'scenarios' / 'pano-scan')
    runner = CliRunner()

    result = runner.invoke(cli, ['scan', 'SF', '--filter', 'METRICS3_STG.ADWORDS_VIEWS.ENTITY%'])

    assert result.exit_code == 0
