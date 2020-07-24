from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from panoramic.cli import cli
from panoramic.cli.local.file_utils import Paths


@pytest.mark.vcr
def test_scan_e2e(monkeypatch):
    test_dir = Path('e2e') / 'scenarios' / 'pano-scan'
    monkeypatch.chdir(test_dir)
    runner = CliRunner()

    # Clean scanned directory
    for f in Paths.scanned_dir().glob('*'):
        f.unlink()

    result = runner.invoke(cli, ['scan', 'SF', '--parallel', '1', '--filter', 'METRICS3_STG.ADWORDS_VIEWS.ENTITY%'])

    print(result.stdout)

    assert result.exit_code == 0
    assert {f.name for f in Paths.scanned_dir().iterdir()} == {
        'sf.metrics3_stg.adwords_views.entity_accounts.model.yaml',
        'sf.metrics3_stg.adwords_views.entity_adgroups.model.yaml',
        'sf.metrics3_stg.adwords_views.entity_adgroups_from_service.model.yaml',
        'sf.metrics3_stg.adwords_views.entity_ads.model.yaml',
        'sf.metrics3_stg.adwords_views.entity_ads_from_service.model.yaml',
        'sf.metrics3_stg.adwords_views.entity_advideos.model.yaml',
        'sf.metrics3_stg.adwords_views.entity_campaigns.model.yaml',
        'sf.metrics3_stg.adwords_views.entity_campaigns_criterion.model.yaml',
        'sf.metrics3_stg.adwords_views.entity_campaigns_from_service.model.yaml',
        'sf.metrics3_stg.adwords_views.entity_user_lists.model.yaml',
    }


@pytest.mark.vcr
@patch('panoramic.cli.command.scan')
def test_scan_error_e2e(mock_scan, monkeypatch):
    monkeypatch.chdir(Path('e2e') / 'scenarios' / 'pano-scan')
    mock_scan.side_effect = Exception('Test Exception')
    runner = CliRunner()

    result = runner.invoke(cli, ['scan', 'SF', '--filter', 'METRICS3_STG.ADWORDS_VIEWS.ENTITY%'])

    assert result.exit_code == 1
    assert result.stdout.startswith('Error: Internal error occurred\nTraceback (most recent call last):\n')
    assert result.stdout.endswith('raise effect\nException: Test Exception\n\n')
