import logging
from typing import Optional

from panoramic.cli.local.file_utils import (
    FileExtension,
    get_target_abs_filepath,
    write_yaml,
)
from panoramic.cli.pano_model import Actionable, PanoDataSource, PanoModel

logger = logging.getLogger(__name__)


class FileWriter:
    """Responsible for writing data to local filesystem."""

    def delete(self, actionable: Actionable):
        """Delete data from local filesystem."""
        if isinstance(actionable, PanoModel):
            return self.delete_model(actionable)
        elif isinstance(actionable, PanoDataSource):
            return self.delete_data_source(actionable)
        else:
            raise NotImplementedError(f'write not implemented for type {type(actionable)}')

    def write(self, actionable: Actionable, *, package: Optional[str] = None):
        """Write data to local filesystem."""
        if isinstance(actionable, PanoModel):
            return self.write_model(actionable, package=package)
        elif isinstance(actionable, PanoDataSource):
            return self.write_data_source(actionable, package=package)
        else:
            raise NotImplementedError(f'write not implemented for type {type(actionable)}')

    def write_data_source(self, data_source: PanoDataSource, *, package: Optional[str] = None):
        """Write data source to local filesystem."""
        if package is None:
            # Default to name of slugified name of DS
            package = data_source.slug
        logger.debug(f'About to write data source {data_source.id}')

    def delete_data_source(self, data_source: PanoDataSource):
        """Delete data source from local filesystem."""
        logger.debug(f'About to delete data source {data_source.id}')

    def write_model(self, model: PanoModel, *, package: Optional[str] = None):
        """Write model to local filesystem."""
        # Default to name of slugified name of DS
        logger.debug(f'About to write model {model.id}')
        package_name = model.virtual_data_source if package is None else package
        assert package_name is not None  # TODO: virtual_data_source is Optional but shouldn't be
        path = get_target_abs_filepath(model.table_file_name, FileExtension.MODEL_YAML, package_name)
        write_yaml(path, model.to_dict())

    def delete_model(self, model: PanoModel):
        """Delete model from local filesystem."""
        logger.debug(f'About to delete model {model.id}')
