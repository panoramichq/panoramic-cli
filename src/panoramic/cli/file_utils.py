import json
import logging
from pathlib import Path
from typing import IO, Any, Dict, List, Optional, Union

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


def write_yaml(abs_filepath: Path, yaml_data: Any):
    """
    Writes yaml dict to path
    """
    logger.debug(f'Write yaml {abs_filepath}')
    ensure_dir(abs_filepath)
    with open(abs_filepath, 'w') as f:
        dump_yaml(yaml_data, f)


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


def append_json_line(abs_filepath: Path, json_dict: Dict[str, Any]) -> None:
    """
    Writes json dict to path as single line. Useful to create JSON lines file. https://jsonlines.org/
    """
    logger.debug(f'Write json {abs_filepath}')
    ensure_dir(abs_filepath)
    with open(abs_filepath, 'a') as f:
        data = json.dumps(json_dict)
        f.write(data + '\n')


def read_json_lines(path: Path) -> List[Dict[str, Any]]:
    """
    Reads json lines from path. https://jsonlines.org/
    """
    logger.debug(f'Read json {path}')
    try:
        json_lines = []
        with open(path, 'r') as f:
            for line in f.readlines():
                json_lines.append(json.loads(line))
        return json_lines
    except FileNotFoundError:
        raise FileMissingError(path=path)


def truncate_file(path: Path) -> None:
    """
    Opening file in write mode will remove files content.
    """
    open(path, 'w').close()


def delete_file(abs_filepath: Path):
    """Delete file at given path."""
    # TODO: Consider warning - wanted to delete model but not found
    if abs_filepath.exists():
        abs_filepath.unlink()
