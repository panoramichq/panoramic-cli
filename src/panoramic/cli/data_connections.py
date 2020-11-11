from typing import Any, Dict, Optional, Tuple

import click
import sqlalchemy  # type: ignore
from tqdm import tqdm

from panoramic.cli.config.storage import read_config, update_config
from panoramic.cli.errors import DataConnectionNotFound
from panoramic.cli.paths import Paths
from panoramic.cli.print import echo_info


def create_data_connection_command(name, type, user, host, port, password, password_stdin, database_name, no_test):
    """CLI command. Create new data connection."""
    data_connections = DataConnections.load()
    if name in data_connections:
        raise click.ClickException(f'Data connection with name "{name}" already exists.')

    if password_stdin:
        password = click.prompt('Enter password: ', hide_input=True, type=str)

    data_connections[name] = {
        'type': type,
        'user': user,
        'password': password,
        'host': host,
        'port': port,
        'database_name': database_name,
    }

    if not no_test:
        ok, error = DataConnections.test(data_connections[name])
        if not ok:
            raise click.ClickException(f'Failed to create data connection: {error}')

    DataConnections.save(data_connections)
    echo_info('Data connection was successfully created!')


def update_data_connection_command(name, type, user, host, port, password, password_stdin, database_name, no_test):
    """CLI command. Update specific data connection."""
    data_connections = DataConnections.load()
    if name not in data_connections:
        raise click.ClickException(f'Data connection with name "{name}" not found.')

    if password_stdin:
        password = click.prompt('Enter password: ', hide_input=True, type=str)

    if type:
        data_connections[name]['type'] = type
    if user:
        data_connections[name]['user'] = user
    if password:
        data_connections[name]['password'] = password
    if host:
        data_connections[name]['host'] = host
    if port:
        data_connections[name]['port'] = port
    if database_name:
        data_connections[name]['database_name'] = database_name

    if not no_test:
        ok, error = DataConnections.test(data_connections[name])
        if not ok:
            raise click.ClickException(f'Failed to create data connection: {error}')

    DataConnections.save(data_connections)
    echo_info('Data connection was successfully created!')


def list_data_connections_command(show_password):
    """CLI command. List all data connections."""
    data_connections = DataConnections.load()
    if not data_connections:
        config_file = Paths.config_file()
        echo_info(
            f'No data connections found.\n'
            f'Use "pano data-connections create" to create data connection or edit "{config_file}" file.'
        )
        exit(0)

    for name, connection in data_connections.items():
        connection_string = DataConnections.create_connection_string(connection)
        if not show_password:
            connection_string = connection_string.replace(connection['password'], '*****')
        echo_info(f'{name}: {connection_string}')


def remove_data_connection_command(name):
    """CLI command. Remove existing data connection."""
    data_connections = DataConnections.load()
    if name not in data_connections:
        raise click.ClickException(f'Data connection with name "{name}" not found.')

    del data_connections[name]
    DataConnections.save(data_connections)


def test_data_connections_command(name: Optional[str] = ''):
    """CLI command. Test data connections by trying to connect to the database.
    Optionally you can specify name for specific connection to that only that."""
    data_connections = DataConnections.load()
    if name != '':
        if name not in data_connections:
            raise click.ClickException(f'Data connection with name "{name}" not found.')
        # Filter specified data connection by name
        data_connections = {name: data_connections[name]}

    progress_bar = tqdm(total=len(data_connections))
    for name, connection in data_connections.items():
        ok, error = DataConnections.test(connection)
        if ok:
            progress_bar.write(f'{name}... OK')
        else:
            progress_bar.write(f'{name}... FAIL: {error}')
        progress_bar.update()


class DataConnections:
    def __init__(self):
        self._connections = self.load()

    def get_by_name_cached(self, name: str) -> Dict[str, str]:
        """Get data connection by name. Use when checking data connections very often, like in for loop.
        The cached data connections will be used instead of loading data connections file every time."""
        if name not in self._connections:
            raise DataConnectionNotFound(name)
        return self._connections[name]

    @classmethod
    def get_by_name(cls, name: str) -> Dict[str, str]:
        data_connections = cls.load()
        return data_connections[name]

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
        """Save data connections YAML."""
        update_config('data_connections', data)

    @staticmethod
    def load() -> Dict[str, Any]:
        """Load data connections YAML."""
        return read_config('data_connections')

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
