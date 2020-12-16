import logging
from typing import Any, Dict, Optional, Tuple

import click
import hologram
import yaml
from dbt.adapters.factory import (
    Credentials,
    get_adapter_class_by_name,
    get_config_class_by_name,
    load_plugin,
)
from dbt.adapters.protocol import AdapterProtocol
from dbt.exceptions import FailedToConnectException, RuntimeException

from panoramic.cli.config.storage import read_config, update_config
from panoramic.cli.errors import (
    ConnectionAlreadyExistsException,
    ConnectionCreateException,
    ConnectionNotFound,
    ConnectionUpdateException,
)
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

logger = logging.getLogger(__name__)


def create_connection_command(
    name: str,
    type: Optional[str],
    user: Optional[str],
    host: Optional[str],
    port: Optional[int],
    password: Optional[str],
    password_stdin: bool,
    database: Optional[str],
    schema: Optional[str],
    warehouse: Optional[str],
    account: Optional[str],
    project: Optional[str],
    key_file: Optional[str],
    no_test: bool,
) -> None:
    """CLI command. Create new connection."""
    connections = Connections.load()
    if name in connections:
        raise ConnectionAlreadyExistsException(name)

    if password_stdin:
        password = click.prompt('Enter password: ', hide_input=True, type=str)

    new_connection: Dict[str, Any] = {}
    _update_connection_from_args(
        new_connection,
        type=type,
        user=user,
        password=password,
        host=host,
        port=port,
        database=database,
        schema=schema,
        warehouse=warehouse,
        account=account,
        project=project,
        key_file=key_file,
    )

    credentials, error = get_dialect_credentials(new_connection)
    if error is not None:
        raise ConnectionCreateException(error)

    if not no_test:
        ok, error = Connections.test(credentials)
        if not ok:
            raise ConnectionCreateException(error)

    # Empty values are important for DBT credentials verification, but we don't need to store empty values in config.
    connections[name] = {}
    for key, value in new_connection.items():
        if value != '':
            connections[name][key] = value

    Connections.save(connections)
    echo_info('Connection was successfully created!')


def update_connection_command(
    name: str,
    type: Optional[str],
    user: Optional[str],
    host: Optional[str],
    port: Optional[int],
    password: Optional[str],
    password_stdin: bool,
    database: Optional[str],
    schema: Optional[str],
    warehouse: Optional[str],
    account: Optional[str],
    project: Optional[str],
    key_file: Optional[str],
    no_test: bool,
) -> None:
    """CLI command. Update specific connection."""
    connections = Connections.load()
    if name not in connections:
        raise ConnectionNotFound(name)

    if password_stdin:
        password = click.prompt('Enter password: ', hide_input=True, type=str)

    new_connection = connections[name].copy()
    _update_connection_from_args(
        new_connection,
        type=type,
        user=user,
        password=password,
        host=host,
        port=port,
        database=database,
        schema=schema,
        warehouse=warehouse,
        account=account,
        project=project,
        key_file=key_file,
    )

    credentials, error = get_dialect_credentials(new_connection)
    if error is not None:
        raise ConnectionUpdateException(error)

    if not no_test:
        ok, error = Connections.test(credentials)
        if not ok:
            raise ConnectionUpdateException(error)

    # Empty values are important for DBT credentials verification, but we don't need to store empty values in config.
    for key, value in new_connection.items():
        if value != '':
            connections[name][key] = value

    Connections.save(connections)
    echo_info('Connection was successfully updated!')


def list_connections_command(show_password: bool) -> None:
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


def remove_connection_command(name: str) -> None:
    """CLI command. Remove existing connection."""
    connections = Connections.load()
    if name not in connections:
        raise ConnectionNotFound(name)

    del connections[name]
    Connections.save(connections)


def test_connections_command(name: Optional[str] = None) -> None:
    """CLI command. Test connections by trying to connect to the database.
    Optionally you can specify name for specific connection to that only that."""
    connections = Connections.load()
    if name is not None:
        if name not in connections:
            raise ConnectionNotFound(name)
        # Filter specified connection by name
        connections = {name: connections[name]}

    for name, connection in connections.items():
        # Fill with default values because DBT requires some fields we don't.
        connection = fill_dbt_required_connection_keys(connection)

        credentials, error = get_dialect_credentials(connection)
        if error is not None:
            echo_info(f'{name}... FAIL: {error}')
            continue

        ok, error = Connections.test(credentials)
        if ok:
            echo_info(f'{name}... OK')
        else:
            echo_info(f'{name}... FAIL: {error}')


class Connections:
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
    def get_connection_adapter(cls, credentials) -> AdapterProtocol:
        """Test connection string by connecting using DBT."""
        # Create dialect specific configuration
        adapter_config_cls = get_config_class_by_name(credentials.type)
        adapter_config = adapter_config_cls()
        adapter_config.credentials = credentials  # type: ignore

        # Create dialect specific adapter that handles connections
        adapter_cls = get_adapter_class_by_name(credentials.type)
        adapter = adapter_cls(adapter_config)  # type: ignore
        return adapter

    @classmethod
    def execute(cls, sql: str, credentials) -> Tuple[str, Any]:
        adapter = cls.get_connection_adapter(credentials)

        with adapter.connection_named('pano'):  # type: ignore
            conn = adapter.connections.get_thread_connection()
            with conn.handle.cursor():
                try:
                    res = adapter.execute(sql=sql, fetch=True)
                    conn.handle.commit()
                except Exception:
                    if conn.handle and getattr(conn.handle, 'closed', None) is not None and conn.handle.closed == 0:
                        conn.handle.rollback()
                    logger.debug(sql)
                    raise
                finally:
                    conn.transaction_open = False

        return res

    @classmethod
    def test(cls, credentials) -> Tuple[bool, str]:
        adapter = cls.get_connection_adapter(credentials)

        # Create dialect specific connection
        connection = adapter.acquire_connection()  # type: ignore
        try:
            # This will try to connect to remote database
            connection.handle
        except FailedToConnectException as e:
            return False, str(e)
        return True, ''


def _update_connection_from_args(
    connection: Dict[str, Any],
    type: Optional[str],
    user: Optional[str],
    host: Optional[str],
    port: Optional[int],
    password: Optional[str],
    database: Optional[str],
    schema: Optional[str],
    warehouse: Optional[str],
    account: Optional[str],
    project: Optional[str],
    key_file: Optional[str],
):
    if type is not None:
        connection['type'] = type
    if user is not None:
        connection['user'] = user
    if password is not None:
        connection['password'] = password
    if host is not None:
        connection['host'] = host
    if port is not None:
        connection['port'] = port
    if database is not None:
        connection['database'] = database
    if schema is not None:
        connection['schema'] = schema
    if warehouse is not None:
        connection['warehouse'] = warehouse
    if account is not None:
        connection['account'] = account
    if project is not None:
        connection['project'] = project
    if key_file is not None:
        connection['key_file'] = key_file

    # Fill with default values because DBT requires some fields we don't.
    return fill_dbt_required_connection_keys(connection)


def fill_dbt_required_connection_keys(connection: Dict[str, Any]) -> Dict[str, Any]:
    for key in CONNECTION_KEYS:
        if key not in connection:
            connection[key] = ''
    return connection


def get_dialect_credentials(connection: Dict[str, Any]) -> (Tuple[Optional[Credentials], Optional[str]]):
    """Use DBT lib to create dialect specific credentials."""
    connection = connection.copy()
    type_ = connection.pop('type')

    try:
        plugin_cls = load_plugin(type_)
    except RuntimeException as e:
        return None, str(e)

    try:
        credentials = plugin_cls.from_dict(connection)
    except hologram.ValidationError as e:
        return None, e.message  # noqa: B306
    return credentials, None
