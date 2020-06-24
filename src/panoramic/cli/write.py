from typing import Any, Dict, Iterable

import yaml


def table_to_yaml(table: Dict) -> str:
    """Transform table record to model YAML."""
    return yaml.safe_dump(table)


def write(tables: Iterable[Dict[str, Any]]):
    """Output tables into file hierarchy."""
    for table in tables:
        name = table['name']
        schema = table['schema']
        file_name = f'{schema}.{name}'
        with open(file_name, 'w') as f:
            f.write(table_to_yaml(table))
