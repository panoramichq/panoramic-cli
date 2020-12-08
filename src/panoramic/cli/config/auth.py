import functools
import os
from typing import Any, Dict, Optional

from panoramic.cli.config.storage import read_config


def get_client_id_env_var() -> str:
    return os.environ['PANO_CLIENT_ID']


def get_client_secret_env_var() -> str:
    return os.environ['PANO_CLIENT_SECRET']


@functools.lru_cache()
def get_client_id() -> str:
    try:
        return get_client_id_env_var()
    except KeyError:
        config = read_config()
        if 'auth' in config:
            return config['auth']['client_id']
        # Backwards compatibility for auth credentials
        return config['client_id']


@functools.lru_cache()
def get_client_secret() -> str:
    try:
        return get_client_secret_env_var()
    except KeyError:
        config = read_config()
        if 'auth' in config:
            return config['auth']['client_secret']
        # Backwards compatibility for auth credentials
        return config['client_secret']


@functools.lru_cache()
def get_dbt_profiles() -> Optional[Dict[str, Any]]:
    return read_config('dbt').get('profiles')
