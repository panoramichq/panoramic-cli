import logging
from typing import Optional

from panoramic.cli.local.file_utils import (
    FileExtension,
    FilePackage,
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

    def write(self, actionable: Actionable):
        """Write data to local filesystem."""
        if isinstance(actionable, PanoModel):
            return self.write_model(actionable)
        elif isinstance(actionable, PanoDataSource):
            return self.write_data_source(actionable)
        else:
            raise NotImplementedError(f'write not implemented for type {type(actionable)}')

    def write_data_source(self, data_source: PanoDataSource):
        """Write data source to local filesystem."""
        logger.debug(f'About to write data source {data_source.id}')

    def delete_data_source(self, data_source: PanoDataSource):
        """Delete data source from local filesystem."""
        logger.debug(f'About to delete data source {data_source.id}')

    def write_model(self, model: PanoModel, *, package: Optional[FilePackage] = None):
        """Write model to local filesystem."""
        logger.debug(f'About to write model {model.id}')
        # TODO: Get package based on dataset?
        package = package or FilePackage.SCANNED
        path = get_target_abs_filepath(model.table_file_name, FileExtension.MODEL_YAML, package)
        write_yaml(path, model.to_dict())

    def delete_model(self, model: PanoModel, *, package: Optional[FilePackage] = None):
        """Delete model from local filesystem."""
        logger.debug(f'About to delete model {model.id}')
