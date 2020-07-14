import pytest
from click.testing import CliRunner

from panoramic.cli import cli


@pytest.fixture(scope="module")
def vcr_config():
    return {"filter_headers": ["authorization"]}


@pytest.fixture(autouse=True)
def setup(monkeypatch):
    monkeypatch.setenv('PANORAMIC_AUTH_TOKEN_URL', 'https://id.panoramichq.com/oauth2/aus5iy3utLXi3Ez4A4x6/v1/token')
    monkeypatch.setenv('PANO_METADATA_BASE_URL', 'https://diesel.panoramicstg.com/api/v1/federated/metadata/')
    monkeypatch.setenv('PANO_MODEL_BASE_URL', 'https://diesel.panoramicstg.com/api/v1/federated/model/')
    monkeypatch.setenv('PANO_VDS_BASE_URL', 'https://diesel.panoramicstg.com/api/v1/federated/virtual-data-source/')


@pytest.mark.vcr
def test_scan():
    runner = CliRunner()

    result = runner.invoke(cli, ['scan', 'sf', '--filter', 'METRICS3_STG.ADWORDS_VIEWS.ENTITY%'])

    assert result.exit_code == 0
    assert result.output == ''
