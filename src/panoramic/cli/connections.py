import logging
from typing import IO, Any, Dict, Optional, Tuple, cast

import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from panoramic.cli.config.storage import read_config, update_config
from panoramic.cli.errors import (
    ConnectionAlreadyExistsException,
    ConnectionCreateException,
    ConnectionNotFound,
    ConnectionUpdateException,
    ExecuteInvalidArgumentsException,
)
from panoramic.cli.husky.service.utils.exceptions import UnknownPhysicalDataSource
from panoramic.cli.paths import Paths
from panoramic.cli.print import echo_info

logger = logging.getLogger(__name__)


def create_connection_command(
    name: str,
    connection_string: str,
    no_test: bool,
) -> None:
    """CLI command. Create new connection."""
    connections = Connections.load()
    if name in connections:
        raise ConnectionAlreadyExistsException(name)

    new_connection = {'connection_string': connection_string}
    connections[name] = new_connection

    if not no_test:
        ok, error = Connections.test(new_connection)
        if not ok:
            raise ConnectionCreateException(error)

    Connections.save(connections)
    echo_info('Connection was successfully created!')


def update_connection_command(
    name: str,
    connection_string: str,
    no_test: bool,
) -> None:
    """CLI command. Update specific connection."""
    connections = Connections.load()
    if name not in connections:
        raise ConnectionNotFound(name)

    new_connection = {'connection_string': connection_string}
    connections[name] = new_connection

    if not no_test:
        ok, error = Connections.test(new_connection)
        if not ok:
            raise ConnectionUpdateException(error)

    for key, value in new_connection.items():
        if value != '':
            connections[name][key] = value

    Connections.save(connections)
    echo_info('Connection was successfully updated!')


def list_connections_command() -> None:
    """CLI command. List all connections."""
    connections = Connections.load()
    if not connections:
        config_file = Paths.config_file()
        echo_info(
            f'No connections found.\n'
            f'Use "pano connection create" to create connection or edit "{config_file}" file.'
        )
        exit(0)

    echo_info(yaml.dump(connections))


def remove_connection_command(name: str) -> None:
    """CLI command. Remove existing connection."""
    connections = Connections.load()
    if name not in connections:
        raise ConnectionNotFound(name)

    del connections[name]
    Connections.save(connections)


def execute_command(
    connection: str,
    query: Optional[str],
    file: Optional[IO],
    type: Optional[str],
    name: Optional[str],
) -> None:
    """CLI command. Update specific connection."""
    connections = Connections.load()
    if connection not in connections:
        raise ConnectionNotFound(connection)

    if (query and file) or (not query and not file):
        raise ExecuteInvalidArgumentsException('Either query or file must be provided but not both.')

    if file:
        query = file.read().decode("utf-8")

    if (type != 'raw') and not name:
        raise ExecuteInvalidArgumentsException(
            'When --type is used, please also set the name of the view or table by providing --name option.'
        )

    if name and type == 'raw':
        raise ExecuteInvalidArgumentsException(
            'When --name is used, please also set the the type of the created object by providing --type argument.'
        )

    if type and name:
        query = f'CREATE OR REPLACE {type} {name} AS {query}'

    return Connections.execute(cast(str, query), connections[connection])


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
        ok, error = Connections.test(connection)
        if ok:
            echo_info(f'{name}... OK')
        else:
            echo_info(f'{name}... FAIL: {error}')


class Connections:
    @classmethod
    def get_by_name(cls, name: str, throw_if_missing: bool = False) -> Dict[str, str]:
        connections = cls.load()
        if name not in connections and throw_if_missing:
            raise UnknownPhysicalDataSource(name)

        return connections[name]

    @classmethod
    def get_connection_string(cls, connection: Dict[str, Any]) -> str:
        """Gets connection string from physical data source connection"""
        return connection['connection_string']

    @staticmethod
    def save(data: Dict[str, Any]) -> None:
        """Save connections YAML."""
        update_config('connections', data)

    @staticmethod
    def load() -> Dict[str, Any]:
        """Load connections YAML."""
        return read_config('connections')

    @classmethod
    def get_connection_engine(cls, connection) -> Engine:
        return create_engine(cls.get_connection_string(connection))

    @classmethod
    def execute(cls, sql: str, connection) -> Any:
        engine = cls.get_connection_engine(connection)

        with engine.connect() as connection:
            return connection.execute(text(sql))

    @classmethod
    def test(cls, connection) -> Tuple[bool, str]:
        engine = cls.get_connection_engine(connection)

        try:
            # This will try to connect to remote database
            with engine.connect():
                pass
        except Exception as e:
            return False, str(e)
        return True, ''
