import logging
from typing import Optional

from panoramic.cli.model import ModelClient
from panoramic.cli.pano_model import Actionable, PanoDataSource, PanoModel
from panoramic.cli.virtual_data_source import VirtualDataSourceClient

logger = logging.getLogger(__name__)


class RemoteWriter:

    """Responsible for writing data to remote API."""

    company_name: str
    virtual_data_source_client: VirtualDataSourceClient
    model_client: ModelClient

    def __init__(
        self,
        company_name: str,
        *,
        virtual_data_source_client: Optional[VirtualDataSourceClient] = None,
        model_client: Optional[ModelClient] = None,
    ):
        if virtual_data_source_client is None:
            virtual_data_source_client = VirtualDataSourceClient()
        if model_client is None:
            model_client = ModelClient()

        self.virtual_data_source_client = virtual_data_source_client
        self.model_client = model_client

    def delete(self, actionable: Actionable):
        """Delete data from remote api."""
        if isinstance(actionable, PanoModel):
            return self.delete_model(actionable)
        elif isinstance(actionable, PanoDataSource):
            return self.delete_data_source(actionable)
        else:
            raise NotImplementedError(f'delete not implemented for type {type(actionable)}')

    def write(self, actionable: Actionable):
        """Write data to remote api."""
        if isinstance(actionable, PanoModel):
            return self.write_model(actionable)
        elif isinstance(actionable, PanoDataSource):
            return self.write_data_source(actionable)
        else:
            raise NotImplementedError(f'write not implemented for type {type(actionable)}')

    def write_data_source(self, data_source: PanoDataSource):
        """Write data source to remote API."""
        logger.debug(f'About to write data source {data_source.id}')
        self.virtual_data_source_client.upsert_virtual_data_source(self.company_name, data_source.to_dict())

    def delete_data_source(self, data_source: PanoDataSource):
        """Delete data source from remote API."""
        logger.debug(f'About to delete data source {data_source.id}')
        self.virtual_data_source_client.upsert_virtual_data_source(self.company_name, data_source.to_dict())

    def write_model(self, model: PanoModel):
        """Write model to remote API."""
        logger.debug(f'About to write model {model.id}')
        # TODO: make virtual_data_source non optional
        assert model.virtual_data_source is not None
        self.model_client.upsert_model(model.virtual_data_source, self.company_name, model.to_dict())

    def delete_model(self, model: PanoModel):
        """Delete model from remote API."""
        logger.debug(f'About to delete model {model.id}')
        # TODO: make virtual_data_source non optional
        assert model.virtual_data_source is not None
        self.model_client.delete_model(model.virtual_data_source, self.company_name, model.table_file_name)
