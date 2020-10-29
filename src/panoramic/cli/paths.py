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
    def dbt_project_dir() -> Path:
        return Path.cwd() / SystemDirectory.DBT.value

    @classmethod
    def dbt_packages_file(cls) -> Path:
        return cls.dbt_project_dir() / PresetFileName.DBT_PACKAGES_FILE.value

    @classmethod
    def dbt_project_file(cls) -> Path:
        return cls.dbt_project_dir() / PresetFileName.DBT_PROJECT_FILE.value

    @staticmethod
    def dotenv_file() -> Path:
        return Path.cwd() / PresetFileName.DOTENV.value

    @staticmethod
    def dbt_config_dir() -> Path:
        return Path.home() / PresetFileName.CONFIG_DIR.value / PresetFileName.DBT_DIR.value

    @classmethod
    def dbt_profiles_file(cls) -> Path:
        return cls.dbt_config_dir() / PresetFileName.DBT_PROFILES_FILE.value

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
    def scanned_fields_dir() -> Path:
        return Paths.fields_dir(Paths.scanned_dir())

    @staticmethod
    def company_fields_dir() -> Path:
        return Path.cwd() / SystemDirectory.FIELDS.value

    @staticmethod
    def fields_dir(package: Path) -> Path:
        return package / SystemDirectory.FIELDS.value

    @staticmethod
    def dataset_schema_file() -> Path:
        with importlib_resources.path(panoramic.cli.schemas, PresetFileName.DATASET_SCHEMA.value) as path:
            return path

    @staticmethod
    def model_schema_file() -> Path:
        with importlib_resources.path(panoramic.cli.schemas, PresetFileName.MODEL_SCHEMA.value) as path:
            return path

    @staticmethod
    def field_schema_file() -> Path:
        with importlib_resources.path(panoramic.cli.schemas, PresetFileName.FIELD_SCHEMA.value) as path:
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
    FIELD_YAML = '.field.yaml'


class PresetFileName(Enum):
    """Enumeration with all available preset file names."""

    DATASET_YAML = 'dataset.yaml'
    CONTEXT = 'pano.yaml'
    DOTENV = '.env'
    CONFIG_DIR = '.pano'
    DBT_DIR = '.dbt'
    CONFIG = 'config'
    MODEL_SCHEMA = 'model.schema.json'
    FIELD_SCHEMA = 'field.schema.json'
    DATASET_SCHEMA = 'dataset.schema.json'
    CONFIG_SCHEMA = 'config.schema.json'
    CONTEXT_SCHEMA = 'context.schema.json'
    DBT_PROJECT_FILE = 'dbt_project.yml'
    DBT_PACKAGES_FILE = 'packages.yml'
    DBT_PROFILES_FILE = 'profiles.yml'


class SystemDirectory(Enum):
    SCANNED = 'scanned'
    FIELDS = 'fields'
    DBT = '.dbt'
