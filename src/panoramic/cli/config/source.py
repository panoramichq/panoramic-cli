import os

BASE_URL = 'https://diesel.panoramicapi.com/api/v1/federated/physical-data-source/'


def get_base_url() -> str:
    return os.environ.get('PANO_PDS_BASE_URL', BASE_URL)
