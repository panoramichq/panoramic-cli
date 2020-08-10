import os

BASE_URL = 'https://diesel.panoramicprod.com/api/v1/federated/identifier/'


def get_base_url() -> str:
    return os.environ.get('PANO_IDENTIFIER_BASE_URL', BASE_URL)
