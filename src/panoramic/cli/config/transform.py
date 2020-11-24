import os

BASE_URL = 'https://diesel.panoramicapi.com/api/v1/federated/transform/'


def get_base_url() -> str:
    return os.environ.get('PANO_TRANSFORM_BASE_URL', BASE_URL)
