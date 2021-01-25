import functools
import itertools
import json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union

import jsonschema
from jsonschema.exceptions import ValidationError as JsonSchemaValidationError

from panoramic.cli.errors import (
    DeprecatedAttributeWarning,
    DeprecatedConfigProperty,
    DuplicateFieldSlugError,
    DuplicateModelNameError,
    InvalidYamlFile,
    JsonSchemaError,
    MissingFieldFileError,
    OrphanFieldFileError,
    ValidationError,
)
from panoramic.cli.file_utils import read_yaml
from panoramic.cli.local.reader import FilePackage, FileReader, GlobalPackage
from panoramic.cli.pano_model import PanoField, PanoModel
from panoramic.cli.paths import Paths
from panoramic.cli.print import echo_warning


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
    def field() -> Dict[str, Any]:
        """Return schema of model files."""
        with Paths.field_schema_file().open('r') as f:
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


def _check_model_deprecations(data: Dict[str, Any], path: Path) -> List[DeprecatedAttributeWarning]:
    """Check for deprecated attributes in a model."""
    errors = []

    fields: List[Dict[str, Any]] = data.get('fields', [])
    contains_data_type = any('data_type' in f for f in fields)
    if contains_data_type:
        errors.append(DeprecatedAttributeWarning(attribute='data_type', path=path))

    return errors


def _check_properties_deprecations(fp: Path, schema: Dict[str, Any]):
    """Check for deprecated properties in config file.
    WARNING: this check currently doesn't support recursive lookup of properties!"""
    errors = []
    data = read_yaml(fp)
    for name, value in schema.get('properties', {}).items():
        if value.get("deprecated", False) and data.get(name) is not None:
            errors.append(DeprecatedConfigProperty(name, value.get("deprecationMessage")))
    return errors


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


def _validate_package_dataset(package: FilePackage) -> List[ValidationError]:
    """Validate all files in a given package."""
    errors: List[ValidationError] = []
    try:
        _validate_data(package.read_data_source(), JsonSchemas.dataset())
    except InvalidYamlFile as e:
        errors.append(e)
    except JsonSchemaValidationError as e:
        errors.append(JsonSchemaError(path=package.data_source_file, error=e))

    return errors


def _validate_package_models(package: FilePackage) -> Tuple[List[PanoModel], List[ValidationError]]:
    """Validate models in a given package."""
    errors: List[ValidationError] = []
    models = []
    model_paths_by_name: Dict[str, List[Path]] = defaultdict(list)
    for model_data, model_path in package.read_models():
        try:
            _validate_data(model_data, JsonSchemas.model())
            model = PanoModel.from_dict(model_data)
            models.append(model)
            model_paths_by_name[model.model_name].append(model_path)
            errors.extend(_check_model_deprecations(model_data, model_path))
        except InvalidYamlFile as e:
            errors.append(e)
        except JsonSchemaValidationError as e:
            errors.append(JsonSchemaError(path=model_path, error=e))

    # check for duplicate model names
    for model_name, paths in model_paths_by_name.items():
        if len(paths) > 1:
            errors.append(DuplicateModelNameError(model_name=model_name, paths=paths))

    return models, errors


def _validate_package_fields(
    package: Union[FilePackage, GlobalPackage]
) -> Tuple[List[PanoField], List[ValidationError]]:
    errors: List[ValidationError] = []
    fields = []
    field_paths_by_id: Dict[Tuple, List[Path]] = defaultdict(list)
    for field_data, field_path in package.read_fields():
        try:
            _validate_data(field_data, JsonSchemas.field())
            field = PanoField.from_dict(field_data)
            fields.append(field)
            field_paths_by_id[(field.data_source, field.slug)].append(field_path)
        except InvalidYamlFile as e:
            errors.append(e)
        except JsonSchemaValidationError as e:
            errors.append(JsonSchemaError(path=field_path, error=e))

    # check for duplicate field slugs
    for (dataset_slug, field_slug), paths in field_paths_by_id.items():
        if len(paths) > 1:
            errors.append(DuplicateFieldSlugError(field_slug=field_slug, dataset_slug=dataset_slug, paths=paths))

    return fields, errors


def validate_missing_files(
    fields: List[PanoField], models: List[PanoModel], package_name: str
) -> List[MissingFieldFileError]:
    """Check for missing field files based on field map in model files."""
    fields_slugs_from_fields = {f.slug for f in fields}
    # take one model for every field
    data_reference_by_field_slugs = {
        field_name: (model.data_source, f.data_reference, field_name in model.identifiers)
        for model in models
        for f in model.fields
        for field_name in f.field_map
    }

    field_slugs_with_no_files = data_reference_by_field_slugs.keys() - fields_slugs_from_fields

    return [
        MissingFieldFileError(
            field_slug=slug,
            dataset_slug=package_name,
            data_source=data_reference_by_field_slugs[slug][0],
            data_reference=data_reference_by_field_slugs[slug][1],
            identifier=data_reference_by_field_slugs[slug][2],
        )
        for slug in field_slugs_with_no_files
    ]


def validate_orphaned_files(
    fields: List[PanoField], models: List[PanoModel], package_name: str
) -> List[OrphanFieldFileError]:
    """Check for field files not linked to any models."""
    # Fields with calculation without model link are normal
    fields_slugs_from_fields = {f.slug for f in fields if f.calculation is None}
    fields_slugs_from_models = set(itertools.chain.from_iterable(f.field_map for model in models for f in model.fields))

    extraneous_field_slugs = fields_slugs_from_fields.difference(fields_slugs_from_models)

    return [OrphanFieldFileError(field_slug=slug, dataset_slug=package_name) for slug in extraneous_field_slugs]


def _validate_package(package: FilePackage) -> List[ValidationError]:
    """Validate all files in a given package."""
    errors: List[ValidationError] = []
    errors.extend(_validate_package_dataset(package))

    models, model_errors = _validate_package_models(package)
    errors.extend(model_errors)

    fields, field_errors = _validate_package_fields(package)
    errors.extend(field_errors)

    missing_file_errors = validate_missing_files(fields, models, package_name=package.name)
    errors.extend(missing_file_errors)

    orphan_file_errors = validate_orphaned_files(fields, models, package_name=package.name)
    errors.extend(orphan_file_errors)

    return errors


def validate_local_state() -> List[ValidationError]:
    """Check local state against defined schemas."""
    file_reader = FileReader()
    packages = file_reader.get_packages()
    _, errors = _validate_package_fields(file_reader.get_global_package())

    executor = ThreadPoolExecutor(max_workers=4)
    for package_errors in executor.map(_validate_package, packages):
        errors.extend(package_errors)

    return errors


def validate_config():
    """Check config file against schema."""
    path, schema = Paths.config_file(), JsonSchemas.config()
    _validate_file(path, schema)
    errors = _check_properties_deprecations(path, schema)
    for err in errors:
        echo_warning(str(err))


def validate_context():
    """Check context file against schema."""
    _validate_file(Paths.context_file(), JsonSchemas.context())
