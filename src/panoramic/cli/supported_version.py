import logging

import requests

from packaging import version


URL = "https://a1.panocdn.com/updates/pano-cli/versions.json"

logger = logging.getLogger(__name__)


def __fetch_minimum_supported_version(current_version) -> str:
    """Fetch minimum supported version of pano-cli from remote server.
    Returns JSON containing info about versions.
    Example:
    {
        "minimum_supported_version": "0.1.0",
        "latest_version": "0.1.0"
    }"""
    response = requests.get(URL, headers={"User-Agent": f"pano-cli/{current_version}"}, timeout=5)
    response.raise_for_status()
    data = response.json()
    return data['minimum_supported_version']


def is_version_supported(current_version: str) -> bool:
    """Check if current version of the CLI is still supported.
    If version has been deprecated print warning message notifying user to update the CLI.
    Returns bool. If check was successful program can continue otherwise it should be stopped.
    """
    try:
        minimum_supported_version = __fetch_minimum_supported_version(current_version)
    except requests.exceptions.RequestException:
        logger.debug("Failed to connect to remote server to verify minimum supported CLI version.", exc_info=True)
        print("ERROR: Failed to connect to remote server to verify minimum supported CLI version.")
        return False
    except (KeyError, TypeError):
        logger.debug("Failed to verify minimum supported CLI version.", exc_info=True)
        print("ERROR: Failed to verify minimum supported CLI version.")
        return False

    if version.parse(current_version) < version.parse(minimum_supported_version):
        message = (
            f"WARNING: This version '{current_version}' has been deprecated.\n"
            f"Please update to version '{minimum_supported_version}' or higher.\n"
            "To update run: `pip install --upgrade pano-cli`.\n"
        )
        print(message)
        return False
    return True
