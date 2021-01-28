import abc
from typing import Dict

from panoramic.cli.connections import Connections
from panoramic.cli.errors import ConnectionNotFound
from panoramic.cli.pano_model import PanoModel, PanoModelField


class BaseScanner:
    """Base scanner of metadata in database engine"""

    def __init__(self, connection_name: str):
        self._models: Dict[str, PanoModel] = {}
        self._model_fields: Dict[str, PanoModelField] = {}

        self._connection_name = connection_name

    def reset(self):
        """Reset scanned metadata"""
        self._models = {}
        self._model_fields = {}

    def _get_connection(self) -> Dict[str, str]:
        """Gets connection data"""
        try:
            return Connections.get_by_name(self._connection_name)
        except ValueError:
            raise ConnectionNotFound(self._connection_name)

    @property
    def models(self) -> Dict[str, PanoModel]:
        """Map of scanned tables"""
        return self._models

    @property
    def model_fields(self) -> Dict[str, PanoModelField]:
        """Map of scanned table columns"""
        return self._model_fields

    @abc.abstractmethod
    def scan(self, *, force_reset: bool = False):
        """Scan the database storage"""
        pass
