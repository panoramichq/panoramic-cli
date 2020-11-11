from typing import Any, Dict, Optional, Tuple

import click
import sqlalchemy  # type: ignore
from tqdm import tqdm

from panoramic.cli.config.storage import read_config, update_config
from panoramic.cli.errors import ConnectionNotFound
from panoramic.cli.paths import Paths
from panoramic.cli.print import echo_info


def create_connection_command(name, type, user, host, port, password, password_stdin, database_name, no_test):
    """CLI command. Create new connection."""
    connections = Connections.load()
    if name in connections:
        raise click.ClickException(f'Connection with name "{name}" already exists.')

    if password_stdin:
        password = click.prompt('Enter password: ', hide_input=True, type=str)

    connections[name] = {
        'type': type,
        'user': user,
        'password': password,
        'host': host,
        'port': port,
        'database_name': database_name,
    }

    if not no_test:
        ok, error = Connections.test(connections[name])
        if not ok:
            raise click.ClickException(f'Failed to create connection: {error}')

    Connections.save(connections)
    echo_info('Connection was successfully created!')


def update_connection_command(name, type, user, host, port, password, password_stdin, database_name, no_test):
    """CLI command. Update specific connection."""
    connections = Connections.load()
    if name not in connections:
        raise click.ClickException(f'Connection with name "{name}" not found.')

    if password_stdin:
        password = click.prompt('Enter password: ', hide_input=True, type=str)

    if type:
        connections[name]['type'] = type
    if user:
        connections[name]['user'] = user
    if password:
        connections[name]['password'] = password
    if host:
        connections[name]['host'] = host
    if port:
        connections[name]['port'] = port
    if database_name:
        connections[name]['database_name'] = database_name

    if not no_test:
        ok, error = Connections.test(connections[name])
        if not ok:
            raise click.ClickException(f'Failed to create connection: {error}')

    Connections.save(connections)
    echo_info('Connection was successfully created!')


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

    for name, connection in connections.items():
        connection_string = Connections.create_connection_string(connection)
        if not show_password:
            connection_string = connection_string.replace(connection['password'], '*****')
        echo_info(f'{name}: {connection_string}')


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

    progress_bar = tqdm(total=len(connections))
    for name, connection in connections.items():
        ok, error = Connections.test(connection)
        if ok:
            progress_bar.write(f'{name}... OK')
        else:
            progress_bar.write(f'{name}... FAIL: {error}')
        progress_bar.update()


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
    def create_connection_string(connection):
        return str(
            sqlalchemy.engine.url.URL(
                connection['type'],
                host=connection['host'],
                port=connection.get('port'),
                username=connection['user'],
                password=connection['password'],
                database=connection.get('database_name'),
            )
        )

    @staticmethod
    def save(data: Dict[str, Any]) -> None:
        """Save connections YAML."""
        update_config('connections', data)

    @staticmethod
    def load() -> Dict[str, Any]:
        """Load connections YAML."""
        return read_config('connections')

    @classmethod
    def test(cls, connection: Dict[str, Any]) -> Tuple[bool, str]:
        """Test connection string by connecting SQLAlchemy engine."""
        connection_string = cls.create_connection_string(connection)
        engine = sqlalchemy.create_engine(connection_string)
        try:
            engine.connect()
            return True, ''
        except sqlalchemy.exc.DatabaseError as e:
            return False, e.orig
