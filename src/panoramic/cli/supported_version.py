import logging
import os

import requests
from packaging import version
from tqdm import tqdm

from panoramic.cli.print import echo_error

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


def __get_upgrade_command() -> str:
    """
    Read environment variables to understand how the CLI was installed.
    Return the appropriate command for the user to upgrade to the newest version.
    """
    if os.environ.get('RUNNING_UNDER_HOMEBREW') is not None:
        return "`brew update && brew upgrade pano`"
    # Always fall back to assuming PIP
    return "`pip install --upgrade pano`"


def is_version_supported(current_version: str) -> bool:
    """Check if current version of the CLI is still supported.
    If version has been deprecated print warning message notifying user to update the CLI.
    Returns bool. If check was successful program can continue otherwise it should be stopped.
    """
    try:
        minimum_supported_version = __fetch_minimum_supported_version(current_version)
    except requests.exceptions.RequestException:
        error_msg = 'Failed to connect to remote server to verify minimum supported CLI version.'
        echo_error(error_msg)
        logger.debug(error_msg, exc_info=True)
        return False
    except (KeyError, TypeError):
        error_msg = 'Failed to verify minimum supported CLI version.'
        echo_error(error_msg)
        logger.debug(error_msg, exc_info=True)
        return False

    if version.parse(current_version) < version.parse(minimum_supported_version):
        upgrade_command = __get_upgrade_command()
        message = (
            f"WARNING: This version '{current_version}' has been deprecated.\n"
            f"Please update to version '{minimum_supported_version}' or higher.\n"
            f"To update run: {upgrade_command}.\n"
        )
        tqdm.write(message)
        return False
    return True
