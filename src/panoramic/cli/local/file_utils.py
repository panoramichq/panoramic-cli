import logging
from enum import Enum
from pathlib import Path
from typing import Any, Dict

import yaml

logger = logging.getLogger(__name__)

API_VERSION_ATTRIBUTE = 'api_version'


class FileExtension(Enum):
    """
    Enumeration with all available file extensions
    """

    MODEL_YAML = '.model.yaml'


class PresetFileName(Enum):

    """Enumeration with all available preset file names."""

    DATASET_YAML = 'dataset.yaml'
    CONTEXT = 'pano.yaml'
    DOTENV = '.env'


class SystemDirectory(Enum):

    SCANNED = 'scanned'


def ensure_dir(abs_filepath: Path):
    """
    Ensure parent directory exists.
    """
    path_obj = abs_filepath.parent
    path_obj.mkdir(parents=True, exist_ok=True)


def write_yaml(abs_filepath: Path, yaml_dict: Dict[str, Any]):
    """
    Writes yaml dict to path
    """
    logger.debug(f'Write yaml {abs_filepath}')
    ensure_dir(abs_filepath)
    with open(abs_filepath, 'w') as f:
        yaml.dump(yaml_dict, f, default_flow_style=False)


def read_yaml(abs_filepath: Path) -> Dict[str, Any]:
    """
    Reads yaml dict from path
    """
    logger.debug(f'Read yaml {abs_filepath}')
    with open(abs_filepath, 'r') as f:
        return yaml.safe_load(f)


def delete_file(abs_filepath: Path):
    """Delete file at given path."""
    # TODO: Consider warning - wanted to delete model but not found
    if abs_filepath.exists():
        abs_filepath.unlink()


def add_file_api_version(data: Dict[str, Any], api_version: str) -> Dict[str, Any]:
    """Add file api version."""
    data[API_VERSION_ATTRIBUTE] = api_version
    return data


def remove_file_api_version(data: Dict[str, Any]) -> Dict[str, Any]:
    """Remove file api version."""
    if API_VERSION_ATTRIBUTE in data:
        del data[API_VERSION_ATTRIBUTE]
    return data
