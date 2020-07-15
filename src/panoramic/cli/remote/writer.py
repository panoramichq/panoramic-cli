import logging

from panoramic.cli.mapper import map_data_source_from_local, map_model_from_local
from panoramic.cli.model import ModelClient
from panoramic.cli.pano_model import Actionable, PanoModel, PanoVirtualDataSource
from panoramic.cli.virtual_data_source import VirtualDataSourceClient

logger = logging.getLogger(__name__)


class RemoteWriter:

    """Responsible for writing data to remote API."""

    company_name: str
    virtual_data_source_client: VirtualDataSourceClient
    model_client: ModelClient

    def __init__(self, company_name: str):
        self.company_name = company_name
        self.virtual_data_source_client = VirtualDataSourceClient()
        self.model_client = ModelClient()

    def delete(self, actionable: Actionable):
        """Delete data from remote api."""
        if isinstance(actionable, PanoModel):
            return self.delete_model(actionable)
        elif isinstance(actionable, PanoVirtualDataSource):
            return self.delete_data_source(actionable)
        else:
            raise NotImplementedError(f'delete not implemented for type {type(actionable)}')

    def write(self, actionable: Actionable):
        """Write data to remote api."""
        if isinstance(actionable, PanoModel):
            return self.write_model(actionable)
        elif isinstance(actionable, PanoVirtualDataSource):
            return self.write_data_source(actionable)
        else:
            raise NotImplementedError(f'write not implemented for type {type(actionable)}')

    def write_data_source(self, data_source: PanoVirtualDataSource):
        """Write data source to remote API."""
        logger.debug(f'About to write data source {data_source.id}')
        remote_data_source = map_data_source_from_local(data_source)
        self.virtual_data_source_client.upsert_virtual_data_source(self.company_name, remote_data_source)

    def delete_data_source(self, data_source: PanoVirtualDataSource):
        """Delete data source from remote API."""
        logger.debug(f'About to delete data source {data_source.id}')
        self.virtual_data_source_client.delete_virtual_data_source(self.company_name, data_source.dataset_slug)

    def write_model(self, model: PanoModel):
        """Write model to remote API."""
        logger.debug(f'About to write model {model.id}')
        # TODO: make virtual_data_source non optional
        assert model.virtual_data_source is not None
        remote_model = map_model_from_local(model)
        self.model_client.upsert_model(model.virtual_data_source, self.company_name, remote_model)

    def delete_model(self, model: PanoModel):
        """Delete model from remote API."""
        logger.debug(f'About to delete model {model.id}')
        # TODO: make virtual_data_source non optional
        assert model.virtual_data_source is not None
        self.model_client.delete_model(model.virtual_data_source, self.company_name, model.model_name)
