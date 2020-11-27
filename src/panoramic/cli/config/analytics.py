import distutils.util
import os
from typing import Optional

import requests

from panoramic.cli.supported_version import fetch_analytics_write_key


def is_enabled() -> bool:
    try:
        enabled = os.environ.get('PANO_ANALYTICS_ENABLED', 'true')
        return distutils.util.strtobool(enabled)
    except ValueError:
        return True


def get_write_key() -> Optional[str]:
    """Get Segment integration source write key."""
    write_key = os.environ.get("PANO_ANALYTICS_WRITE_KEY", None)
    if write_key is not None:
        return write_key

    try:
        write_key = fetch_analytics_write_key()
        if write_key is not None:
            return write_key
    except requests.exceptions.RequestException:
        pass

    return None
