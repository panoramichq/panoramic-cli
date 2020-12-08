from pathlib import Path
from unittest.mock import call, patch

from panoramic.cli.dbt import prepare_dbt_project
from panoramic.cli.paths import Paths


@patch('panoramic.cli.dbt.get_dbt_vars')
@patch('panoramic.cli.dbt.get_dbt_target_parts')
@patch('panoramic.cli.dbt.get_dbt_target_name')
@patch('panoramic.cli.dbt.get_dbt_packages')
@patch('panoramic.cli.dbt.write_yaml')
@patch('panoramic.cli.dbt.Connections.load')
def test_prepare_dbt_project(
    mock_connections_load,
    mock_write_yaml,
    mock_get_dbt_packages,
    mock_get_dbt_target_name,
    mock_get_dbt_target_parts,
    mock_get_dbt_vars,
):
    mock_connections_load.return_value = {
        'default-target': {
            'user': 'test-user',
            'password': 'test-password',
            'host': 'test-host',
        }
    }
    mock_get_dbt_target_name.return_value = 'default-target'
    mock_get_dbt_target_parts.return_value = {
        'schema': 'test-schema',
        'database': 'test-database',
    }
    mock_get_dbt_packages.return_value = [
        {'local': 'tmp/package'},
        {'package': 'panoramichq/dbt-test', 'version': '0.1.0'},
        {'git': 'https://github.com/fishtown-analytics/dbt-utils.git', 'revision': 'master'},
    ]
    mock_get_dbt_vars.return_value = {'var1': 'val1'}

    prepare_dbt_project()

    assert mock_write_yaml.mock_calls == [
        call(
            Paths.dbt_profiles_file(),
            {
                'default': {
                    'outputs': {
                        'default-target': {
                            'user': 'test-user',
                            'password': 'test-password',
                            'host': 'test-host',
                            'schema': 'test-schema',
                            'database': 'test-database',
                        }
                    },
                    'target': 'default-target',
                }
            },
        ),
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
