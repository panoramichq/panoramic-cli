import logging

import requests

from panoramic.cli.errors import (
    DatasetWriteException,
    FieldWriteException,
    InvalidDatasetException,
    InvalidFieldException,
    InvalidModelException,
    ModelWriteException,
)
from panoramic.cli.field import FieldClient
from panoramic.cli.field_mapper import map_field_from_local
from panoramic.cli.model import ModelClient
from panoramic.cli.model_mapper import map_data_source_from_local, map_model_from_local
from panoramic.cli.pano_model import (
    Actionable,
    PanoField,
    PanoModel,
    PanoVirtualDataSource,
)
from panoramic.cli.virtual_data_source import VirtualDataSourceClient

logger = logging.getLogger(__name__)


class RemoteWriter:

    """Responsible for writing data to remote API."""

    company_name: str
    virtual_data_source_client: VirtualDataSourceClient
    model_client: ModelClient
    field_client: FieldClient

    def __init__(self, company_name: str):
        self.company_name = company_name
        self.virtual_data_source_client = VirtualDataSourceClient()
        self.model_client = ModelClient()
        self.field_client = FieldClient()

    def delete(self, actionable: Actionable):
        """Delete data from remote api."""
        if isinstance(actionable, PanoModel):
            return self.delete_model(actionable)
        elif isinstance(actionable, PanoVirtualDataSource):
            return self.delete_data_source(actionable)
        else:
            raise NotImplementedError(f'delete not implemented for type {type(actionable)}')

    def write(self, actionable: Actionable):
        if isinstance(actionable, PanoModel):
            return self.write_model(actionable)
        elif isinstance(actionable, PanoVirtualDataSource):
            return self.write_data_source(actionable)
        elif isinstance(actionable, PanoField):
            return self.write_field(actionable)
        else:
            raise NotImplementedError(f'write not implemented for type {type(actionable)}')

    def write_data_source(self, data_source: PanoVirtualDataSource):
        """Write data source to remote API."""
        logger.debug(f'About to write data source {data_source.id}')
        remote_data_source = map_data_source_from_local(data_source)
        try:
            self.virtual_data_source_client.upsert_virtual_data_source(self.company_name, remote_data_source)
        except requests.RequestException as e:
            logger.debug(f'Failed to write dataset {data_source.dataset_slug}', exc_info=True)
            if e.response is not None and e.response.status_code == requests.codes.bad_request:
                raise InvalidDatasetException(e).extract_request_id(e)
            raise DatasetWriteException(data_source.dataset_slug).extract_request_id(e)

    def delete_data_source(self, data_source: PanoVirtualDataSource):
        """Delete data source from remote API."""
        logger.debug(f'About to delete data source {data_source.id}')
        try:
            self.virtual_data_source_client.delete_virtual_data_source(self.company_name, data_source.dataset_slug)
        except requests.RequestException as e:
            logger.debug(f'Failed to delete dataset {data_source.dataset_slug}', exc_info=True)
            raise DatasetWriteException(data_source.dataset_slug).extract_request_id(e)

    def write_model(self, model: PanoModel):
        """Write model to remote API."""
        logger.debug(f'About to write model {model.id}')
        # TODO: make virtual_data_source non optional
        assert model.virtual_data_source is not None
        remote_model = map_model_from_local(model)
        try:
            self.model_client.upsert_model(model.virtual_data_source, self.company_name, remote_model)
        except requests.RequestException as e:
            logger.debug(
                f'Failed to write model {model.model_name} in dataset {model.virtual_data_source}', exc_info=True
            )
            if e.response is not None and e.response.status_code == requests.codes.bad_request:
                raise InvalidModelException(e).extract_request_id(e)
            raise ModelWriteException(model.virtual_data_source, model.model_name).extract_request_id(e)

    def delete_model(self, model: PanoModel):
        """Delete model from remote API."""
        logger.debug(f'About to delete model {model.id}')
        # TODO: make virtual_data_source non optional
        assert model.virtual_data_source is not None
        try:
            self.model_client.delete_model(model.virtual_data_source, self.company_name, model.model_name)
        except requests.RequestException as e:
            logger.debug(
                f'Failed to delete model {model.model_name} in dataset {model.virtual_data_source}', exc_info=True
            )
            raise ModelWriteException(model.virtual_data_source, model.model_name).extract_request_id(e)

    def write_field(self, field: PanoField):
        """Write model on remote API."""
        logger.debug(f'About to write field {field.slug} in dataset {field.data_source}')

        remote_field = map_field_from_local(field)
        try:
            self.field_client.upsert_fields(company_slug=self.company_name, fields=[remote_field])
        except requests.RequestException as e:
            logger.debug(f'Failed to write field {field.slug} in dataset {field.data_source}', exc_info=True)
            if e.response is not None and e.response.status_code == requests.codes.bad_request:
                raise InvalidFieldException(e).extract_request_id(e)
            raise FieldWriteException(field.data_source, field.slug).extract_request_id(e)

    def delete_field(self, field: PanoField):
        """Delete model from remote API."""
        logger.debug(f'About to delete field {field.slug}')

        try:
            remote_field = map_field_from_local(field)
            self.field_client.delete_fields(company_slug=self.company_name, slugs=[remote_field.slug])
        except requests.RequestException as e:
            logger.debug(f'Failed to delete field {field.slug} in dataset {field.data_source}', exc_info=True)
            raise FieldWriteException(field.data_source, field.slug).extract_request_id(e)
