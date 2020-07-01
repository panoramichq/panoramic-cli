import os

from pathlib import Path

import yaml


def get_client_id() -> str:
    try:
        return os.environ['PANO_CLIENT_ID']
    except KeyError:
        with open(Path.home() / '.pano' / 'config') as f:
            return yaml.safe_load(f)['client_id']


def get_client_secret() -> str:
    try:
        return os.environ['PANO_CLIENT_SECRET']
    except KeyError:
        with open(Path.home() / '.pano' / 'config') as f:
            return yaml.safe_load(f)['client_secret']


def get_token_url() -> str:
    try:
        return os.environ['PANO_TOKEN_URL']
    except KeyError:
        with open(Path.home() / '.pano' / 'config') as f:
            return yaml.safe_load(f)['token_url']
