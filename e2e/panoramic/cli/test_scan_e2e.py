import json

import pytest
from click.testing import CliRunner

from panoramic.cli import cli

_TEST_JWT = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c'


def scrub_access_token(response):
    """Scrub access token from auth server response."""
    if b'access_token' in response['body']['string']:
        body = json.loads(response['body']['string'])
        body['access_token'] = _TEST_JWT
        response['body']['string'] = json.dumps(body)
    return response


@pytest.fixture(scope="module")
def vcr_config():
    return {"filter_headers": ["authorization"], 'before_record_response': scrub_access_token}


@pytest.fixture(autouse=True)
def setup(monkeypatch):
    monkeypatch.setenv('PANORAMIC_AUTH_TOKEN_URL', 'https://id.panoramichq.com/oauth2/aus5iy3utLXi3Ez4A4x6/v1/token')
    monkeypatch.setenv('PANO_METADATA_BASE_URL', 'https://diesel.panoramicstg.com/api/v1/federated/metadata/')
    monkeypatch.setenv('PANO_MODEL_BASE_URL', 'https://diesel.panoramicstg.com/api/v1/federated/model/')
    monkeypatch.setenv('PANO_VDS_BASE_URL', 'https://diesel.panoramicstg.com/api/v1/federated/virtual-data-source/')


@pytest.mark.vcr
def test_scan_e2e():
    runner = CliRunner()

    result = runner.invoke(cli, ['--debug', 'scan', 'SF', '--filter', 'METRICS3_STG.ADWORDS_VIEWS.ENTITY%'])

    assert result.exit_code == 0
    assert result.output == ''
