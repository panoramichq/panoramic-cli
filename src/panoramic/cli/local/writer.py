import logging
from pathlib import Path
from typing import Optional

from tqdm import tqdm

from panoramic.cli.local.file_utils import (
    FileExtension,
    PresetFileName,
    add_file_api_version,
    delete_file,
    write_yaml,
)
from panoramic.cli.pano_model import Actionable, PanoModel, PanoVirtualDataSource

logger = logging.getLogger(__name__)


class FileWriter:
    """Responsible for writing data to local filesystem."""

    API_VERSION = 'v1'
    cwd: Path

    def __init__(self, *, cwd: Optional[Path] = None):
        if cwd is None:
            cwd = Path.cwd()

        self.cwd = cwd

    def delete(self, actionable: Actionable):
        """Delete data from local filesystem."""
        if isinstance(actionable, PanoModel):
            return self.delete_model(actionable)
        elif isinstance(actionable, PanoVirtualDataSource):
            return self.delete_data_source(actionable)
        else:
            raise NotImplementedError(f'write not implemented for type {type(actionable)}')

    def write(self, actionable: Actionable, *, package: Optional[str] = None):
        """Write data to local filesystem."""
        if isinstance(actionable, PanoModel):
            return self.write_model(actionable, package=package)
        elif isinstance(actionable, PanoVirtualDataSource):
            return self.write_data_source(actionable, package=package)
        else:
            raise NotImplementedError(f'write not implemented for type {type(actionable)}')

    def write_data_source(self, data_source: PanoVirtualDataSource, *, package: Optional[str] = None):
        """Write data source to local filesystem."""
        # Default to name of slugified name of DS
        package = package if package is not None else data_source.dataset_slug
        path = self.cwd / package / PresetFileName.DATASET_YAML.value
        logger.debug(f'About to write data source {data_source.id}')
        write_yaml(path, add_file_api_version(data_source.to_dict(), self.API_VERSION))
        tqdm.write(f'Updated dataset {data_source.dataset_slug}')

    def delete_data_source(self, data_source: PanoVirtualDataSource):
        """Delete data source from local filesystem."""
        assert data_source.package is not None
        path = self.cwd / data_source.package / PresetFileName.DATASET_YAML.value
        logger.debug(f'About to delete data source {data_source.id}')
        delete_file(path)
        tqdm.write(f'Deleted dataset {data_source.dataset_slug}')

    def write_model(self, model: PanoModel, *, package: Optional[str] = None):
        """Write model to local filesystem."""
        # Default to name of slugified name of DS
        package_name = model.virtual_data_source if package is None else package
        assert package_name is not None  # TODO: virtual_data_source is Optional but shouldn't be
        path = self.cwd / package_name / f'{model.model_name}{FileExtension.MODEL_YAML.value}'
        logger.debug(f'About to write model {model.id}')
        write_yaml(path, add_file_api_version(model.to_dict(), self.API_VERSION))
        tqdm.write(f'Updated model {model.model_name}')

    def delete_model(self, model: PanoModel):
        """Delete model from local filesystem."""
        assert model.package is not None
        path = self.cwd / model.package / f'{model.model_name}{FileExtension.MODEL_YAML.value}'
        logger.debug(f'About to delete model {model.id}')
        delete_file(path)
        tqdm.write(f'Deleted model {model.model_name}')
