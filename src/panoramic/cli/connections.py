from typing import Any, Dict, Optional, Tuple

import click
import hologram
import yaml

from panoramic.cli.config.storage import read_config, update_config
from panoramic.cli.errors import ConnectionNotFound
from panoramic.cli.paths import Paths
from panoramic.cli.print import echo_info

CONNECTION_KEYS = [
    'type',
    'user',
    'host',
    'port',
    'password',
    'database',
    'schema',
    'warehouse',
    'account',
    'project',
    'key_file',
]


def create_connection_command(
    name,
    type,
    user,
    host,
    port,
    password,
    password_stdin,
    database,
    schema,
    warehouse,
    account,
    project,
    key_file,
    no_test,
):
    """CLI command. Create new connection."""
    connections = Connections.load()
    if name in connections:
        raise click.ClickException(f'Connection with name "{name}" already exists.')

    if password_stdin:
        password = click.prompt('Enter password: ', hide_input=True, type=str)

    new_connection = {
        'type': type,
        'user': user,
        'host': host,
        'port': port,
        'password': password,
        'database': database,
        'schema': schema,
        'warehouse': warehouse,
        'account': account,
        'project': project,
        'key_file': key_file,
    }

    credentials, error = get_dialect_credentials(new_connection)
    if error != '':
        raise click.ClickException(f'Failed to create connection: {error}')

    if not no_test:
        ok, error = Connections.test(credentials)
        if not ok:
            raise click.ClickException(f'Failed to create connection: {error}')

    # Empty values are important for DBT credentials verification, but we don't need to store empty values in config.
    connections[name] = {}
    for key, value in new_connection.items():
        if value:
            connections[name][key] = value

    Connections.save(connections)
    echo_info('Connection was successfully created!')


def update_connection_command(
    name,
    type,
    user,
    host,
    port,
    password,
    password_stdin,
    database,
    schema,
    warehouse,
    account,
    project,
    key_file,
    no_test,
):
    """CLI command. Update specific connection."""
    connections = Connections.load()
    if name not in connections:
        raise click.ClickException(f'Connection with name "{name}" not found.')

    if password_stdin:
        password = click.prompt('Enter password: ', hide_input=True, type=str)

    new_connection = connections[name].copy()
    if type:
        new_connection['type'] = type
    if user:
        new_connection['user'] = user
    if password:
        new_connection['password'] = password
    if host:
        new_connection['host'] = host
    if port:
        new_connection['port'] = port
    if database:
        new_connection['database'] = database
    if schema:
        new_connection['schema'] = schema
    if warehouse:
        new_connection['warehouse'] = warehouse
    if account:
        new_connection['account'] = account
    if project:
        new_connection['project'] = project
    if key_file:
        new_connection['key_file'] = key_file

    # Fill with default values because DBT requires some fields we don't.
    for key in CONNECTION_KEYS:
        if key not in new_connection:
            new_connection[key] = ''

    credentials, error = get_dialect_credentials(new_connection)
    if error != '':
        raise click.ClickException(f'Failed to update connection: {error}.')

    if not no_test:
        ok, error = Connections.test(credentials)
        if not ok:
            raise click.ClickException(f'Failed to update connection: {error}')

    # Empty values are important for DBT credentials verification, but we don't need to store empty values in config.
    for key, value in new_connection.items():
        if value:
            connections[name][key] = value

    Connections.save(connections)
    echo_info('Connection was successfully updated!')


def list_connections_command(show_password):
    """CLI command. List all connections."""
    connections = Connections.load()
    if not connections:
        config_file = Paths.config_file()
        echo_info(
            f'No connections found.\n'
            f'Use "pano connection create" to create connection or edit "{config_file}" file.'
        )
        exit(0)

    if not show_password:
        for conn in connections.values():
            conn['password'] = '*****'
    echo_info(yaml.dump(connections))


def remove_connection_command(name):
    """CLI command. Remove existing connection."""
    connections = Connections.load()
    if name not in connections:
        raise click.ClickException(f'Connection with name "{name}" not found.')

    del connections[name]
    Connections.save(connections)


def test_connections_command(name: Optional[str] = ''):
    """CLI command. Test connections by trying to connect to the database.
    Optionally you can specify name for specific connection to that only that."""
    connections = Connections.load()
    if name != '':
        if name not in connections:
            raise click.ClickException(f'Connection with name "{name}" not found.')
        # Filter specified connection by name
        connections = {name: connections[name]}

    for name, connection in connections.items():
        # Fill with default values because DBT requires some fields we don't.
        for key in CONNECTION_KEYS:
            if key not in connection:
                connection[key] = ''

        credentials, error = get_dialect_credentials(connection)
        if error != '':
            echo_info(f'{name}... FAIL: {error}')
            continue

        ok, error = Connections.test(credentials)
        if ok:
            echo_info(f'{name}... OK')
        else:
            echo_info(f'{name}... FAIL: {error}')


class Connections:
    def __init__(self):
        self._connections = self.load()

    def get_by_name_cached(self, name: str) -> Dict[str, str]:
        """Get connection by name. Use when checking connections very often, like in for loop.
        The cached connections will be used instead of loading connections file every time."""
        if name not in self._connections:
            raise ConnectionNotFound(name)
        return self._connections[name]

    @classmethod
    def get_by_name(cls, name: str) -> Dict[str, str]:
        connections = cls.load()
        return connections[name]

    @staticmethod
    def save(data: Dict[str, Any]) -> None:
        """Save connections YAML."""
        update_config('connections', data)

    @staticmethod
    def load() -> Dict[str, Any]:
        """Load connections YAML."""
        return read_config('connections')

    @classmethod
    def test(cls, credentials) -> Tuple[bool, str]:
        """Test connection string by connecting using DBT."""
        from dbt.adapters.factory import (
            get_adapter_class_by_name,
            get_config_class_by_name,
        )
        from dbt.exceptions import FailedToConnectException

        # Create dialect specific configuration
        adapter_config_cls = get_config_class_by_name(credentials.type)
        adapter_config = adapter_config_cls()
        adapter_config.credentials = credentials  # type: ignore

        # Create dialect specific adapter that handles connections
        adapter_cls = get_adapter_class_by_name(credentials.type)
        adapter = adapter_cls(adapter_config)  # type: ignore

        # Create dialect specific connection
        connection = adapter.acquire_connection()  # type: ignore
        try:
            # This will try to connect to remote database
            connection.handle
        except FailedToConnectException as e:
            return False, str(e)
        return True, ''


def get_dialect_credentials(connection: Dict[str, Any]):
    """Use DBT lib to create dialect specific credentials."""
    from dbt.adapters.factory import load_plugin
    from dbt.exceptions import RuntimeException

    connection = connection.copy()
    type_ = connection.pop('type')

    try:
        plugin_cls = load_plugin(type_)
    except RuntimeException as e:
        return None, str(e)

    try:
        credentials = plugin_cls.from_dict(connection)
    except hologram.ValidationError as e:
        return False, e.message  # noqa: B306
    return credentials, ''
