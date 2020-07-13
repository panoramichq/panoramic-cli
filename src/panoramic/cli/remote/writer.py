import logging

from panoramic.cli.model import ModelClient
from panoramic.cli.pano_model import Actionable, PanoDataSource, PanoModel
from panoramic.cli.virtual_data_source import VirtualDataSourceClient

logger = logging.getLogger(__name__)


class ApiWriter:
    """Responsible for writing data to remote API."""

    def __init__(self):
        self.virtual_data_source_client = VirtualDataSourceClient()
        self.model_client = ModelClient()

    def delete(self, actionable: Actionable, company_name: str):
        """Delete data from remote api."""
        if isinstance(actionable, PanoModel):
            return self.delete_model(actionable, company_name)
        elif isinstance(actionable, PanoDataSource):
            return self.delete_data_source(actionable, company_name)
        else:
            raise NotImplementedError(f'write not implemented for type {type(actionable)}')

    def write(self, actionable: Actionable, company_name: str):
        """Write data to remote api."""
        if isinstance(actionable, PanoModel):
            return self.write_model(actionable, company_name)
        elif isinstance(actionable, PanoDataSource):
            return self.write_data_source(actionable, company_name)
        else:
            raise NotImplementedError(f'write not implemented for type {type(actionable)}')

    def write_data_source(self, data_source: PanoDataSource, company_name: str):
        """Write data source to remote API."""
        logger.debug(f'About to write data source {data_source.id}')
        self.virtual_data_source_client.upsert_virtual_data_source(company_name, data_source)

    def delete_data_source(self, data_source: PanoDataSource, company_name: str):
        """Delete data source from remote API."""
        logger.debug(f'About to delete data source {data_source.id}')
        self.virtual_data_source_client.upsert_virtual_data_source(company_name, data_source.slug)

    def write_model(self, model: PanoModel, company_name: str):
        """Write model to remote API."""
        logger.debug(f'About to write model {model.id}')
        self.model_client.upsert_model(model.virtual_data_source, company_name, model)

    def delete_model(self, model: PanoModel, company_name: str):
        """Delete model from remote API."""
        logger.debug(f'About to delete model {model.id}')
        self.model_client.delete_model(model.virtual_data_source, company_name, model.table_file_name)
