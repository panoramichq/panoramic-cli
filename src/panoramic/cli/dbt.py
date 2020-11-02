from pathlib import Path

from panoramic.cli.config.auth import get_dbt_profiles
from panoramic.cli.context import get_dbt_packages, get_dbt_vars
from panoramic.cli.file_utils import write_yaml
from panoramic.cli.paths import Paths


def prepare_dbt_project():
    """Set up DBT project for DBT CLI execution."""

    # Create the config file
    profiles = get_dbt_profiles()
    if profiles is not None:
        write_yaml(Paths.dbt_profiles_file(), profiles)

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
