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
            'setup',
            '--url',
            'sqlite://',
            '--no-test',
        ],
    )

    assert result.exit_code == 0, result.output
    connections_json = {
        'connection': {
            'url': 'sqlite://',
        },
    }
    with Paths.context_file().open() as f:
        assert yaml.safe_load(f.read()) == connections_json

    # List
    result = runner.invoke(cli, ['connection', 'show'])
    assert result.exit_code == 0, result.output
    assert result.output == yaml.dump(connections_json['connection']) + "\n"

    # Update
    result = runner.invoke(cli, ['connection', 'setup', '--url', 'sqlite://'])
    assert result.exit_code == 0, result.output

    # List
    result = runner.invoke(cli, ['connection', 'show'])
    assert result.exit_code == 0, result.output
    connections_json['connection']['url'] = 'sqlite://'
    assert result.output == yaml.dump(connections_json['connection']) + "\n"

    # Ensure no traces of the connections are left
    remove_context()


def test_connections_list_fail_e2e(monkeypatch, tmpdir):
    monkeypatch.setattr(Path, 'home', lambda: Path(tmpdir))
    runner = CliRunner()

    # Ensure starting from scratch.
    remove_context()

    # List connections
    result = runner.invoke(cli, ['connection', 'show'])
    assert result.exit_code == 0, result.output
    assert result.stdout.startswith('No connection setup yet.')


def remove_context():
    try:
        os.remove(Paths.context_file())
    except FileNotFoundError:
        pass
