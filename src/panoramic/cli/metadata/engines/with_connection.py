import abc
from typing import Dict

from panoramic.cli.connection import Connection
from panoramic.cli.errors import ConnectionNotFound
from panoramic.cli.metadata.engines.base import BaseScanner


class WithConnection(BaseScanner, metaclass=abc.ABCMeta):
    """Base scanner of metadata in database engine with connection name"""

    def __init__(self):
        super().__init__()

    def _get_connection(self) -> Dict[str, str]:
        """Gets connection data"""
        try:
            return Connection.get()
        except ValueError:
            raise ConnectionNotFound()
