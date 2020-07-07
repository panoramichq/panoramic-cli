import os

# FIXME: move under federated when https://github.com/panoramichq/diesel-service/pull/1047 is ready
BASE_URL = 'https://diesel.panoramicprod.com/api/v1/virtual-data-source/'


def get_base_url() -> str:
    return os.environ.get('PANO_METADATA_BASE_URL', BASE_URL)
