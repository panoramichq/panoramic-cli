import logging

from typing import Any, Dict, Iterable

import yaml


logger = logging.getLogger(__name__)


def table_to_yaml(table: Dict) -> str:
    """Transform table record to model YAML."""
    return yaml.safe_dump(table)


def write(tables: Iterable[Dict[str, Any]]):
    """Output tables into file hierarchy."""
    for table in tables:
        logger.debug(f'About to write table {table}')
        name = table['name']
        schema = table['schema']
        file_name = f'{schema}.{name}'
        with open(file_name, 'w') as f:
            f.write(table_to_yaml(table))
