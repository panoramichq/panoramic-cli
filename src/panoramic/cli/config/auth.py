import os
from pathlib import Path

from panoramic.cli.errors import MissingConfigFileException
from panoramic.cli.util import get_yaml_value


def _get_config_yaml_value(file_path: Path, value_path: str):
    try:
        return get_yaml_value(file_path, value_path)
    except FileNotFoundError:
        raise MissingConfigFileException()


def get_client_id() -> str:
    try:
        return os.environ['PANO_CLIENT_ID']
    except KeyError:
        return _get_config_yaml_value(Path.home() / '.pano' / 'config', 'client_id')


def get_client_secret() -> str:
    try:
        return os.environ['PANO_CLIENT_SECRET']
    except KeyError:
        return _get_config_yaml_value(Path.home() / '.pano' / 'config', 'client_secret')
