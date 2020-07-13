import os

# TODO: add /federated once Diesel change released
BASE_URL = 'https://diesel.panoramicprod.com/api/v1/virtual-data-source/'


def get_base_url() -> str:
    return os.environ.get('PANO_VDS_BASE_URL', BASE_URL)
