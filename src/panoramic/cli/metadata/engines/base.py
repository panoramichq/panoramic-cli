import abc
from typing import Dict

from panoramic.cli.pano_model import PanoModel, PanoModelField


class BaseScanner(metaclass=abc.ABCMeta):
    """Base scanner of metadata in database engine"""

    def __init__(self):
        self._models: Dict[str, PanoModel] = {}
        self._model_fields: Dict[str, PanoModelField] = {}

    def reset(self):
        """Reset scanned metadata"""
        self._models = {}
        self._model_fields = {}

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
