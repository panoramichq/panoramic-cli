import functools
import json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List

import jsonschema
from jsonschema.exceptions import ValidationError as JsonSchemaValidationError

from panoramic.cli.config.auth import get_client_id_env_var, get_client_secret_env_var
from panoramic.cli.errors import (
    DuplicateModelNameError,
    InvalidYamlFile,
    JsonSchemaError,
    ValidationError,
)
from panoramic.cli.file_utils import read_yaml
from panoramic.cli.local.reader import FilePackage, FileReader
from panoramic.cli.pano_model import PanoModel
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


def _validate_data(data: Dict[str, Any], schema: Dict[str, Any]):
    """Validate file against schema."""
    jsonschema.validate(data, schema)


def _validate_file(fp: Path, schema: Dict[str, Any]):
    """Validate file against schema."""
    try:
        data = read_yaml(fp)
        _validate_data(data, schema)
    except JsonSchemaValidationError as e:
        raise JsonSchemaError(path=fp, error=e)


def _validate_package(package: FilePackage) -> List[ValidationError]:
    """Validate all files in a given package."""
    errors: List[ValidationError] = []
    try:
        _validate_data(package.read_data_source(), JsonSchemas.dataset())
    except InvalidYamlFile as e:
        errors.append(e)
    except JsonSchemaValidationError as e:
        errors.append(JsonSchemaError(path=package.data_source_file, error=e))

    model_paths_by_name: Dict[str, List[Path]] = defaultdict(list)

    for model_data, model_path in package.read_models():
        try:
            _validate_data(model_data, JsonSchemas.model())
            model = PanoModel.from_dict(model_data)
            model_paths_by_name[model.model_name].append(model_path)
        except InvalidYamlFile as e:
            errors.append(e)
        except JsonSchemaValidationError as e:
            errors.append(JsonSchemaError(path=model_path, error=e))

    for model_name, paths in model_paths_by_name.items():
        if len(paths) > 1:
            errors.append(DuplicateModelNameError(model_name=model_name, paths=paths))

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
