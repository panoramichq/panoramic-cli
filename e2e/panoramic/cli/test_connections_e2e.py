import os
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

    # Create connection
    result = runner.invoke(
        cli,
        [
            'connection',
            'create',
            'my-connection',
            '--url' '"sqlite://"',
            '--no-test',
        ],
    )

    assert result.exit_code == 0, result.output
    connections_json = {
        'connections': {
            'my-connection': {
                'url': 'sqlite://',
            },
        },
    }
    with Paths.context_file().open() as f:
        assert yaml.safe_load(f.read()) == connections_json

    # List
    result = runner.invoke(cli, ['connection', 'list'])
    assert result.exit_code == 0, result.output
    assert result.output == yaml.dump(connections_json['connections']) + "\n"

    # Update
    result = runner.invoke(cli, ['connection', 'update', 'my-connection', '--url', '"sqlite://"'])
    assert result.exit_code == 0, result.output

    # List
    result = runner.invoke(cli, ['connection', 'list'])
    assert result.exit_code == 0, result.output
    connections_json['connections']['my-connection']['url'] = 'sqlite://'
    assert result.output == yaml.dump(connections_json['connections']) + "\n"

    # Update
    result = runner.invoke(cli, ['connection', 'remove', 'my-connection'])
    assert result.exit_code == 0, result.output

    # List
    result = runner.invoke(cli, ['connection', 'list'])
    assert result.exit_code == 0, result.output
    assert result.stdout.startswith('No connections found.\nUse "pano connection create" to create')

    # Ensure no traces of the connections are left
    remove_context()


def test_connections_list_fail_e2e(monkeypatch, tmpdir):
    monkeypatch.setattr(Path, 'home', lambda: Path(tmpdir))
    runner = CliRunner()

    # Ensure starting from scratch.
    remove_context()

    # List connections
    result = runner.invoke(cli, ['connection', 'list'])
    assert result.exit_code == 0, result.output
    assert result.stdout.startswith('No connections found.\nUse "pano connection create" to create')


@patch('sqlalchemy.engine.create_engine')
def test_connections_update_fail_e2e(_, monkeypatch, tmpdir):
    monkeypatch.setattr(Path, 'home', lambda: Path(tmpdir))
    runner = CliRunner()

    # Ensure starting from scratch.
    remove_context()

    # Update connection
    result = runner.invoke(cli, ['connection', 'update', 'my-connection', '--url', 'xxx', '--no-test'])
    assert result.exit_code == 1, result.output
    assert result.stdout.endswith('Error: Connection with name "my-connection" was not found.\n')


def remove_context():
    try:
        os.remove(Paths.context_file())
    except FileNotFoundError:
        pass
