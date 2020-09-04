import logging
from pathlib import Path
from typing import IO, Any, Dict, Optional, Union

import yaml

from panoramic.cli.errors import FileMissingError, InvalidYamlFile

logger = logging.getLogger(__name__)


def load_yaml(text: Union[bytes, IO[bytes], str, IO[str]]) -> Any:
    """Load YAML from stream."""
    return yaml.safe_load(text)


def dump_yaml(data: Any, stream: Optional[IO[str]] = None) -> Optional[str]:
    """Dump YAML to stream or return as string."""
    if stream is not None:
        return yaml.safe_dump(data, stream, default_flow_style=False)
    else:
        return yaml.safe_dump(data, default_flow_style=False)


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
        dump_yaml(yaml_dict, f)


def read_yaml(path: Path) -> Dict[str, Any]:
    """
    Reads yaml dict from path
    """
    logger.debug(f'Read yaml {path}')
    try:
        with open(path, 'r') as f:
            return load_yaml(f)
    except FileNotFoundError:
        raise FileMissingError(path=path)
    except yaml.MarkedYAMLError as e:
        raise InvalidYamlFile(path=path, error=e)


def delete_file(abs_filepath: Path):
    """Delete file at given path."""
    # TODO: Consider warning - wanted to delete model but not found
    if abs_filepath.exists():
        abs_filepath.unlink()
