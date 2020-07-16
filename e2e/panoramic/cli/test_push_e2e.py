from pathlib import Path

import pytest
from click.testing import CliRunner

from panoramic.cli import cli


@pytest.mark.vcr
def test_push_e2e(monkeypatch):
    monkeypatch.chdir(Path('e2e') / 'scenarios' / 'pano-push')

    runner = CliRunner()
    result = runner.invoke(cli, ['push'])

    assert result.exit_code == 0
