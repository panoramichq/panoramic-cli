import os

BASE_URL = 'https://diesel.panoramicapi.com/api/v1/federated/metadata/'


def get_base_url() -> str:
    return os.environ.get('PANO_METADATA_BASE_URL', BASE_URL)
