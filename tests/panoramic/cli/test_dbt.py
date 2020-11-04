from pathlib import Path
from unittest.mock import call, patch

from panoramic.cli.dbt import prepare_dbt_project
from panoramic.cli.paths import Paths


@patch('panoramic.cli.dbt.get_dbt_vars')
@patch('panoramic.cli.dbt.get_dbt_profiles')
@patch('panoramic.cli.dbt.get_dbt_packages')
@patch('panoramic.cli.dbt.write_yaml')
def test_prepare_dbt_project(
    mock_write_yaml,
    mock_get_dbt_packages,
    mock_get_dbt_profiles,
    mock_get_dbt_vars,
):
    mock_get_dbt_profiles.return_value = [
        {'test': 1},
    ]
    mock_get_dbt_packages.return_value = [
        {'local': 'tmp/package'},
        {'package': 'panoramichq/dbt-test', 'version': '0.1.0'},
        {'git': 'https://github.com/fishtown-analytics/dbt-utils.git', 'revision': 'master'},
    ]
    mock_get_dbt_vars.return_value = {'var1': 'val1'}

    prepare_dbt_project()

    assert mock_write_yaml.mock_calls == [
        call(Paths.dbt_profiles_file(), [{'test': 1}]),
        call(
            Paths.dbt_packages_file(),
            {
                'packages': [
                    {'local': str(Path.cwd() / 'tmp/package')},
                    {'package': 'panoramichq/dbt-test', 'version': '0.1.0'},
                    {'git': 'https://github.com/fishtown-analytics/dbt-utils.git', 'revision': 'master'},
                ]
            },
        ),
        call(
            Paths.dbt_project_file(),
            {
                'profile': 'default',
                'name': 'pano_dbt_temp',
                'version': '1.0.0',
                'config-version': 2,
                'models': {'+materialized': 'view'},
                'vars': {'var1': 'val1'},
            },
        ),
    ]
