import os
from pathlib import Path
from typing import Optional

import yaml


def get_token() -> Optional[str]:
    return os.environ.get('PANO_AUTH_TOKEN')


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
