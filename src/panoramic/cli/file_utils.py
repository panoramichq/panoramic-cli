import logging
import os

from enum import Enum
from pathlib import Path
from typing import Dict

import yaml


logger = logging.getLogger(__name__)


class FileExtension(Enum):
    """
    Enumeration with all available file extensions
    """

    model_yaml = '.model.yaml'


class FilePackage(Enum):
    """
    Enumeration with all available file packages
    """

    scanned = 'scanned'


def get_work_dir_abs_filepath() -> str:
    """
    Return abs filepath of current workdir
    """
    return str(Path().absolute())


def get_target_abs_filepath(table_file_id: str, file_extension: FileExtension, file_package: FilePackage) -> str:
    """
    Get target file abs filepath
    """
    file_name = f'{table_file_id}{file_extension.value}'
    path_components = (get_work_dir_abs_filepath(), file_package.value, file_name)
    return os.path.join(*path_components)


def ensure_dir(abs_filepath: str) -> None:
    """
    Creates dir if not exist
    """
    dir_path, _ = os.path.split(abs_filepath)
    path_obj = Path(dir_path)
    path_obj.mkdir(parents=True, exist_ok=True)


def write_file(abs_filepath: str, file_content: str) -> None:
    """
    Writes string file to path
    """
    with open(abs_filepath, 'w') as f:
        f.write(file_content)


def read_file(abs_filepath: str) -> str:
    """
    Reads string file from path
    """
    with open(abs_filepath, 'r') as f:
        return f.read()


def to_yaml(yaml_dict: Dict) -> str:
    """
    Serialize to yaml
    """
    return yaml.dump(yaml_dict, default_flow_style=False)


def from_yaml(yaml_str: str) -> Dict:
    """
    Serialize from yaml
    """
    return yaml.safe_load(yaml_str)


def write_yaml(abs_filepath: str, yaml_dict: Dict) -> None:
    """
    Writes yaml dict to path
    """
    logger.debug(f'Write yaml {abs_filepath}')
    ensure_dir(abs_filepath)
    yaml_str = to_yaml(yaml_dict)
    write_file(abs_filepath, yaml_str)


def read_yaml(abs_filepath: str) -> Dict:
    """
    Reads yaml dict from path
    """
    logger.debug(f'Read yaml {abs_filepath}')
    yaml_str = read_file(abs_filepath)
    return from_yaml(yaml_str)
