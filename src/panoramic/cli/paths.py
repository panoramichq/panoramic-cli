import sys

import panoramic.cli.schemas

if sys.version_info >= (3, 7):
    from importlib import resources as importlib_resources
else:
    import importlib_resources as importlib_resources

from enum import Enum
from pathlib import Path


class Paths:
    @staticmethod
    def context_file() -> Path:
        return Path.cwd() / PresetFileName.CONTEXT.value

    @staticmethod
    def dotenv_file() -> Path:
        return Path.cwd() / PresetFileName.DOTENV.value

    @staticmethod
    def config_dir() -> Path:
        return Path.home() / PresetFileName.CONFIG_DIR.value

    @classmethod
    def config_file(cls) -> Path:
        return cls.config_dir() / PresetFileName.CONFIG.value

    @staticmethod
    def scanned_dir() -> Path:
        return Path.cwd() / SystemDirectory.SCANNED.value

    @staticmethod
    def dataset_schema_file() -> Path:
        with importlib_resources.path(panoramic.cli.schemas, PresetFileName.DATASET_SCHEMA.value) as path:
            return path

    @staticmethod
    def model_schema_file() -> Path:
        with importlib_resources.path(panoramic.cli.schemas, PresetFileName.MODEL_SCHEMA.value) as path:
            return path

    @staticmethod
    def config_schema_file() -> Path:
        with importlib_resources.path(panoramic.cli.schemas, PresetFileName.CONFIG_SCHEMA.value) as path:
            return path

    @staticmethod
    def context_schema_file() -> Path:
        with importlib_resources.path(panoramic.cli.schemas, PresetFileName.CONTEXT_SCHEMA.value) as path:
            return path


class FileExtension(Enum):
    """
    Enumeration with all available file extensions
    """

    MODEL_YAML = '.model.yaml'


class PresetFileName(Enum):

    """Enumeration with all available preset file names."""

    DATASET_YAML = 'dataset.yaml'
    CONTEXT = 'pano.yaml'
    DOTENV = '.env'
    CONFIG_DIR = '.pano'
    CONFIG = 'config'
    MODEL_SCHEMA = 'model.schema.json'
    DATASET_SCHEMA = 'dataset.schema.json'
    CONFIG_SCHEMA = 'config.schema.json'
    CONTEXT_SCHEMA = 'context.schema.json'


class SystemDirectory(Enum):

    SCANNED = 'scanned'
