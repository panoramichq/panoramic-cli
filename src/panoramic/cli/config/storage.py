from typing import Any, Dict

from panoramic.cli.file_utils import read_yaml, write_yaml
from panoramic.cli.paths import Paths

# project config file manipulation utils


def update_context(section: str, data: Dict[str, Any]) -> None:
    data = {section: data}
    config_file = Paths.context_file()
    if config_file.is_file():
        config_yaml = read_yaml(config_file)
        config_yaml.update(data)
        write_yaml(config_file, config_yaml)
    else:
        write_yaml(config_file, data)


def read_context(section: str = '') -> Dict[str, Any]:
    config_file = Paths.context_file()
    if not config_file.is_file():
        return {}

    config = read_yaml(config_file)
    if section != '':
        return config.get(section, {})
    return config
