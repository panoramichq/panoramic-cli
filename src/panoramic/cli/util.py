from pathlib import Path
from typing import Any, Dict, List

import pydash
import yaml
import yaml.scanner

from panoramic.cli.errors import InvalidYamlFile, MissingValueException
import re
import unicodedata


def slug_string(input_str: str) -> str:
    """
    Returns lowercase slug variant of given string
    """
    normalized = unicodedata.normalize('NFKD', input_str).encode('ascii', 'ignore').decode('utf8')
    return re.sub('[^a-zA-Z0-9_.]', '', normalized).lower()


def _get_yaml_value_from_object(data: Dict[str, Any], value_path: List[str]):
    try:
        value = data[value_path[0]]
        if len(value_path) > 1:
            return _get_yaml_value_from_object(value, value_path[:1])
        else:
            return value
    except KeyError:
        raise MissingValueException('api_version')


def get_yaml_value(file_path: Path, value_path: str):
    try:
        with open(file_path) as f:
            data = yaml.safe_load(f)
            return _get_yaml_value_from_object(data, value_path.split('.'))
    except yaml.scanner.ScannerError as error:
        raise InvalidYamlFile(error)
