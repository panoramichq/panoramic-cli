import os
from typing import Any, Dict

from panoramic.cli.file_utils import read_yaml, write_yaml
from panoramic.cli.paths import Paths


def update_config(section: str, data: Dict[str, Any]) -> None:
    if section != '':
        data = {section: data}

    config_file = Paths.config_file()
    if os.path.isfile(config_file):
        config_yaml = read_yaml(config_file)
        config_yaml.update(data)
        write_yaml(config_file, config_yaml)
    else:
        write_yaml(config_file, data)


def read_config(section: str = '') -> Dict[str, Any]:
    config = read_yaml(Paths.config_file())
    if section != '':
        return config.get(section, {})
    return config
