import json

import pytest

_TEST_JWT = (
    'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.'
    'eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.'
    'SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c'
)


@pytest.fixture(autouse=True)
def setup(monkeypatch):
    monkeypatch.setenv('PANORAMIC_AUTH_TOKEN_URL', 'https://id.panoramichq.com/oauth2/aus5iy3utLXi3Ez4A4x6/v1/token')
    monkeypatch.setenv('PANO_METADATA_BASE_URL', 'https://diesel.panoramicstg.com/api/v1/federated/metadata/')
    monkeypatch.setenv('PANO_MODEL_BASE_URL', 'https://diesel.panoramicstg.com/api/v1/federated/model/')
    monkeypatch.setenv('PANO_VDS_BASE_URL', 'https://diesel.panoramicstg.com/api/v1/federated/virtual-data-source/')
    monkeypatch.setenv('PANO_PDS_BASE_URL', 'https://diesel.panoramicstg.com/api/v1/federated/physical-data-source/')
    monkeypatch.setenv('PANO_COMPANIES_BASE_URL', 'https://diesel.panoramicstg.com/api/v1/federated/companies/')
    monkeypatch.setenv('PANO_IDENTIFIER_BASE_URL', 'https://diesel.panoramicstg.com/api/v1/federated/identifier/')


def scrub_access_token(response):
    """Scrub access token from auth server response."""
    if b'access_token' in response['body']['string']:
        body = json.loads(response['body']['string'])
        body['access_token'] = _TEST_JWT
        response['body']['string'] = json.dumps(body).encode()
    return response


@pytest.fixture(scope='session')
def vcr_config():
    return {'filter_headers': ['authorization'], 'before_record_response': scrub_access_token}
