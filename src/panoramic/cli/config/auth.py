import os


def get_client_id() -> str:
    return os.environ['PANO_CLIENT_ID']


def get_client_secret() -> str:
    return os.environ['PANO_CLIENT_SECRET']
