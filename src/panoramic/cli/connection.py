import logging
from typing import IO, Any, Dict, Optional, Tuple, cast

import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from panoramic.cli.config.storage import read_context, update_context
from panoramic.cli.errors import (
    ConnectionCreateException,
    ConnectionUrlNotAvailableFound,
    ExecuteInvalidArgumentsException,
)
from panoramic.cli.print import echo_info

logger = logging.getLogger(__name__)


def setup_connection_command(
    url: Optional[str],
    dialect: Optional[str],
    no_test: bool,
) -> None:
    """CLI command. Create new connection."""
    if (url and dialect) or (not url and not dialect):
        raise ConnectionCreateException('Must specify either a URL or dialect, not both.')

    if url:
        connection = {'url': url}
    elif dialect:
        connection = {'dialect': dialect}

    if url and not no_test:
        ok, error = Connection.test(connection)
        if not ok:
            raise ConnectionCreateException(error)

    Connection.save(connection)
    echo_info('Connection was successfully created!')


def show_connection_command() -> None:
    """CLI command. List all connections."""
    connection = Connection.get()
    if not connection:
        echo_info(
            'No connection setup yet.\nUse "pano connection setup" to configure connection or edit pano.yaml file.'
        )
        exit(0)

    echo_info(yaml.dump(connection))


def execute_command(
    query: Optional[str],
    file: Optional[IO],
    type: Optional[str],
    name: Optional[str],
) -> None:
    """CLI command. Update specific connection."""
    connection = Connection.get()

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

    return Connection.execute(cast(str, query), connection)


def test_connection_command() -> None:
    """CLI command. Test connection by trying to connect to the database."""
    connection = Connection.get()
    ok, error = Connection.test(connection)
    if ok:
        echo_info('{name}... OK')
    else:
        echo_info(f'FAIL: {error}')


class Connection:
    @classmethod
    def get_url(cls, connection: Dict[str, Any]) -> str:
        """Gets connection string from physical data source connection"""
        try:
            return connection['url']
        except KeyError:
            raise ConnectionUrlNotAvailableFound()

    @staticmethod
    def save(data: Dict[str, Any]) -> None:
        """Save connection YAML."""
        update_context('connection', data)

    @staticmethod
    def get() -> Dict[str, Any]:
        """Load connection YAML."""
        return read_context('connection')

    @classmethod
    def get_connection_engine(cls, connection) -> Engine:
        return create_engine(cls.get_url(connection))

    @classmethod
    def get_dialect_name(cls, connection) -> str:
        try:
            return connection['dialect']
        except KeyError:
            return create_engine(cls.get_url(connection)).dialect.name

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
