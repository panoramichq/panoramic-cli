import functools
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List

import jsonschema
from jsonschema.exceptions import ValidationError as JsonSchemaValidationError

from panoramic.cli.config.auth import get_client_id_env_var, get_client_secret_env_var
from panoramic.cli.errors import InvalidYamlFile, JsonSchemaError, ValidationError
from panoramic.cli.file_utils import read_yaml
from panoramic.cli.local.reader import FilePackage, FileReader
from panoramic.cli.paths import Paths


class JsonSchemas:
    @staticmethod
    @functools.lru_cache()
    def dataset() -> Dict[str, Any]:
        """Return schema for dataset files."""
        with Paths.dataset_schema_file().open('r') as f:
            return json.load(f)

    @staticmethod
    @functools.lru_cache()
    def model() -> Dict[str, Any]:
        """Return schema of model files."""
        with Paths.model_schema_file().open('r') as f:
            return json.load(f)

    @staticmethod
    @functools.lru_cache()
    def config() -> Dict[str, Any]:
        with Paths.config_schema_file().open('r') as f:
            return json.load(f)

    @staticmethod
    @functools.lru_cache()
    def context():
        """Return schema of context file."""
        with Paths.context_schema_file().open('r') as f:
            return json.load(f)


def _validate_file(fp: Path, schema: Dict[str, Any]):
    """Validate file against schema."""
    try:
        data = read_yaml(fp)
        jsonschema.validate(data, schema)
    except JsonSchemaValidationError as e:
        raise JsonSchemaError(path=fp, error=e)


def _validate_package(package: FilePackage) -> List[ValidationError]:
    """Validate all files in a given package."""
    errors = []
    try:
        _validate_file(package.data_source_file, JsonSchemas.dataset())
    except (ValidationError, InvalidYamlFile) as e:
        errors.append(e)

    for model_file in package.model_files:
        try:
            _validate_file(model_file, JsonSchemas.model())
        except (ValidationError, InvalidYamlFile) as e:
            errors.append(e)

    return errors


def validate_local_state() -> List[ValidationError]:
    """Check local state against defined schemas."""
    packages = FileReader().get_packages()
    errors = []

    executor = ThreadPoolExecutor(max_workers=4)
    for package_errors in executor.map(_validate_package, packages):
        errors.extend(package_errors)

    return errors


def validate_config():
    """Check config file against schema."""
    try:
        _validate_file(Paths.config_file(), JsonSchemas.config())
    except ValidationError as e:
        try:
            # Valid if we get both values from env vars
            get_client_id_env_var()
            get_client_secret_env_var()
        except Exception:
            # Raise original exception if env vars don't help
            raise e


def validate_context():
    """Check context file against schema."""
    _validate_file(Paths.context_file(), JsonSchemas.context())
