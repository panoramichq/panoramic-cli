import abc
from typing import Dict

from panoramic.cli.connections import Connections
from panoramic.cli.errors import ConnectionNotFound
from panoramic.cli.metadata.engines.base import BaseScanner


class WithConnection(BaseScanner, metaclass=abc.ABCMeta):
    """Base scanner of metadata in database engine with connection name"""

    def __init__(self, connection_name: str):
        super().__init__()
        self._connection_name = connection_name

    def _get_connection(self) -> Dict[str, str]:
        """Gets connection data"""
        try:
            return Connections.get_by_name(self._connection_name)
        except ValueError:
            raise ConnectionNotFound(self._connection_name)
