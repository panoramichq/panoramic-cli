from pathlib import Path
from unittest.mock import patch

import yaml
from click.testing import CliRunner

from panoramic.cli import cli
from panoramic.cli.paths import Paths


@patch('sqlalchemy.engine.create_engine')
def test_connections_e2e(mock_create_engine, monkeypatch, tmpdir):
    monkeypatch.setattr(Path, 'home', lambda: Path(tmpdir))
    runner = CliRunner()

    # Create config
    runner.invoke(cli, ['configure'])

    # Create connection
    result = runner.invoke(
        cli,
        [
            'connection',
            'create',
            'my-connection',
            'sqlite://',
            '--no-test',
        ],
    )

    assert result.exit_code == 0, result.output
    connections_json = {
        'auth': {},
        'connections': {
            'my-connection': {
                'connection_string': 'sqlite://',
            },
        },
    }
    with Paths.config_file().open() as f:
        assert yaml.safe_load(f.read()) == connections_json

    # List
    result = runner.invoke(cli, ['connection', 'list'])
    assert result.exit_code == 0, result.output
    assert result.output == yaml.dump(connections_json['connections']) + "\n"

    # Update
    result = runner.invoke(cli, ['connection', 'update', 'my-connection', 'sqlite://'])
    assert result.exit_code == 0, result.output

    # List
    result = runner.invoke(cli, ['connection', 'list'])
    assert result.exit_code == 0, result.output
    connections_json['connections']['my-connection']['connection_string'] = 'sqlite://'
    assert result.output == yaml.dump(connections_json['connections']) + "\n"

    # Update
    result = runner.invoke(cli, ['connection', 'remove', 'my-connection'])
    assert result.exit_code == 0, result.output

    # List
    result = runner.invoke(cli, ['connection', 'list'])
    assert result.exit_code == 0, result.output
    assert result.stdout.startswith('No connections found.\nUse "pano connection create" to create')


def test_connections_list_fail_e2e(monkeypatch, tmpdir):
    monkeypatch.setattr(Path, 'home', lambda: Path(tmpdir))
    runner = CliRunner()

    # Create config
    runner.invoke(cli, ['configure'])

    # List connections
    result = runner.invoke(cli, ['connection', 'list'])
    assert result.exit_code == 0, result.output
    assert result.stdout.startswith('No connections found.\nUse "pano connection create" to create')


@patch('sqlalchemy.engine.create_engine')
def test_connections_update_fail_e2e(_, monkeypatch, tmpdir):
    monkeypatch.setattr(Path, 'home', lambda: Path(tmpdir))
    runner = CliRunner()

    # Create config
    runner.invoke(cli, ['configure'])

    # Update connection
    result = runner.invoke(cli, ['connection', 'update', 'my-connection', 'xxx'])
    assert result.exit_code == 1, result.output
    assert result.stdout.endswith('Error: Connection with name "my-connection" was not found.\n')
