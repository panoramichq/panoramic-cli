import logging
from pathlib import Path
from typing import Optional

from panoramic.cli.file_utils import delete_file, write_yaml
from panoramic.cli.pano_model import (
    Actionable,
    PanoField,
    PanoModel,
    PanoVirtualDataSource,
)
from panoramic.cli.paths import FileExtension, Paths, PresetFileName

logger = logging.getLogger(__name__)


class FileWriter:
    """Responsible for writing data to local filesystem."""

    cwd: Path

    def __init__(self, *, cwd: Optional[Path] = None):
        if cwd is None:
            cwd = Path.cwd()

        # TODO: bunch of methods below ignore self.cwd
        self.cwd = cwd

    def delete(self, actionable: Actionable):
        """Delete data from local filesystem."""
        if isinstance(actionable, PanoModel):
            return self.delete_model(actionable)
        elif isinstance(actionable, PanoVirtualDataSource):
            return self.delete_data_source(actionable)
        elif isinstance(actionable, PanoField):
            return self.delete_field(actionable)
        else:
            raise NotImplementedError(f'write not implemented for type {type(actionable)}')

    def write(self, actionable: Actionable, *, package: Optional[str] = None, file_name: Optional[str] = None):
        """Write data to local filesystem."""
        if isinstance(actionable, PanoModel):
            return self.write_model(actionable, package=package, file_name=file_name)
        elif isinstance(actionable, PanoVirtualDataSource):
            return self.write_data_source(actionable, package=package)
        elif isinstance(actionable, PanoField):
            return self.write_field(actionable, package=package, file_name=file_name)
        else:
            raise NotImplementedError(f'write not implemented for type {type(actionable)}')

    def write_data_source(self, data_source: PanoVirtualDataSource, *, package: Optional[str] = None):
        """Write data source to local filesystem."""
        # Default to name of slugified name of DS
        package = package if package is not None else data_source.dataset_slug
        path = self.cwd / package / PresetFileName.DATASET_YAML.value
        logger.debug(f'About to write data source {data_source.id}')
        write_yaml(path, data_source.to_dict())

    def delete_data_source(self, data_source: PanoVirtualDataSource):
        """Delete data source from local filesystem."""
        assert data_source.package is not None
        path = self.cwd / data_source.package / PresetFileName.DATASET_YAML.value
        logger.debug(f'About to delete data source {data_source.id}')
        delete_file(path)

    def write_empty_model(self, model_name: str):
        """Create an empty model file."""
        path = Paths.scanned_dir() / f'{model_name}{FileExtension.MODEL_YAML.value}'
        logger.debug(f'About to create an empty model {model_name}')
        path.touch()

    def write_empty_field(self, field_slug: str):
        """Create an empty field file."""
        path = Paths.scanned_fields_dir() / f'{field_slug}{FileExtension.FIELD_YAML.value}'
        logger.debug(f'About to create an empty field {field_slug}')
        path.touch()

    def write_scanned_model(self, model: PanoModel):
        """Write scanned model to local filesystem."""
        path = Paths.scanned_dir() / f'{model.model_name}{FileExtension.MODEL_YAML.value}'
        logger.debug(f'About to write model {model.id}')
        write_yaml(path, model.to_dict())

    def write_scanned_field(self, field: PanoField):
        """"Write scanned field to local filesystem."""
        path = Paths.scanned_fields_dir() / f'{field.slug}{FileExtension.FIELD_YAML.value}'
        logger.debug(f'About to write field {field.slug}')
        write_yaml(path, field.to_dict())

    def write_model(self, model: PanoModel, *, package: Optional[str] = None, file_name: Optional[str] = None):
        """Write model to local filesystem."""
        # Default to name of slugified name of DS
        package_name = model.virtual_data_source if package is None else package
        assert package_name is not None  # TODO: virtual_data_source is Optional but shouldn't be
        if file_name is None:
            file_name = f'{model.model_name}{FileExtension.MODEL_YAML.value}'
        path = self.cwd / package_name / file_name
        logger.debug(f'About to write model {model.id}')
        write_yaml(path, model.to_dict())

    def delete_model(self, model: PanoModel):
        """Delete model from local filesystem."""
        assert model.package is not None
        assert model.file_name is not None
        path = self.cwd / model.package / model.file_name
        logger.debug(f'About to delete model {model.id}')
        delete_file(path)

    def write_field(self, field: PanoField, *, package: Optional[str] = None, file_name: Optional[str] = None):
        """Write model to local filesystem."""
        if file_name is None:
            file_name = f'{field.slug}{FileExtension.FIELD_YAML.value}'

        package = package if package is not None else field.data_source
        if package is not None:
            # dataset-scope field
            path = Paths.fields_dir(self.cwd / package) / file_name
        else:
            # company-scope field
            path = Paths.fields_dir(self.cwd) / file_name

        logger.debug(f'About to write field {field.id}')
        write_yaml(path, field.to_dict())

    def delete_field(self, field: PanoField):
        """Delete field from local filesystem."""
        assert field.file_name is not None

        if field.package is not None:
            # dataset-scope field
            path = Paths.fields_dir(self.cwd / field.package) / field.file_name
        else:
            # company-scope field
            path = Paths.fields_dir(self.cwd) / field.file_name

        logger.debug(f'About to delete field {field.id}')
        delete_file(path)
