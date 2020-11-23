from pathlib import Path

from panoramic.cli.connections import Connections
from panoramic.cli.context import (
    get_dbt_packages,
    get_dbt_target_name,
    get_dbt_target_parts,
    get_dbt_vars,
)
from panoramic.cli.file_utils import write_yaml
from panoramic.cli.paths import Paths


def prepare_dbt_project():
    """Set up DBT project for DBT CLI execution."""

    # Find the target connection
    target_name = get_dbt_target_name()
    target_parts = get_dbt_target_parts()

    # Create the config file
    connections = Connections.load()
    for connection_name, connection_data in connections.items():
        if connection_name == target_name:
            connections[connection_name] = {**connection_data, **target_parts}

    write_yaml(Paths.dbt_profiles_file(), {'default': {'outputs': connections, 'target': target_name}})

    # Create the package file
    packages = get_dbt_packages()

    # convert local package paths to be absolute
    for package in packages:
        if 'local' in package:
            package['local'] = str(Path(package['local']).absolute())

    if packages is not None:
        write_yaml(Paths.dbt_packages_file(), {'packages': packages})

    # Create the project file
    project_data = {
        # Default values used in dbt_project.yml
        'profile': 'default',
        'name': 'pano_dbt_temp',
        'version': '1.0.0',
        'config-version': 2,
        # Use SQL views for all projections
        'models': {'+materialized': 'view'},
    }

    dbt_vars = get_dbt_vars()
    if dbt_vars is not None:
        project_data['vars'] = dbt_vars

    write_yaml(Paths.dbt_project_file(), project_data)
