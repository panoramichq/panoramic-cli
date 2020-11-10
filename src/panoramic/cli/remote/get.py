import itertools
from typing import Iterable, Optional

import requests
from requests.exceptions import RequestException

from panoramic.cli.errors import (
    CompanyNotFoundException,
    DatasetReadException,
    FieldReadException,
    ModelReadException,
)
from panoramic.cli.field.client import FieldClient
from panoramic.cli.field_mapper import map_field_from_remote
from panoramic.cli.model import ModelClient
from panoramic.cli.model_mapper import (
    map_data_source_from_remote,
    map_model_from_remote,
)
from panoramic.cli.pano_model import PanoField, PanoModel, PanoVirtualDataSource
from panoramic.cli.state import VirtualState
from panoramic.cli.virtual_data_source import VirtualDataSourceClient


def get_data_sources(company_slug: str, *, limit: int = 100) -> Iterable[PanoVirtualDataSource]:
    """Get virtual data sources from remote."""
    client = VirtualDataSourceClient()
    offset = 0
    while True:
        try:
            sources = client.get_all_virtual_data_sources(company_slug, offset=offset, limit=limit)
        except RequestException as e:
            if e.response is not None and e.response.status_code == requests.codes.not_found:
                raise CompanyNotFoundException(company_slug).extract_request_id(e)
            raise DatasetReadException(company_slug).extract_request_id(e)

        yield from (map_data_source_from_remote(s) for s in sources)
        if len(sources) < limit:
            # last page
            break

        offset += limit


def get_models(data_source: str, company_slug: str, *, limit: int = 100) -> Iterable[PanoModel]:
    """Get all models from remote."""
    client = ModelClient()
    offset = 0
    while True:
        try:
            models = client.get_models(data_source, company_slug, offset=offset, limit=limit)
        except RequestException as e:
            if e.response is not None and e.response.status_code == requests.codes.not_found:
                raise CompanyNotFoundException(company_slug).extract_request_id(e)
            raise ModelReadException(company_slug, data_source).extract_request_id(e)

        yield from (map_model_from_remote(m) for m in models)
        if len(models) < limit:
            # last page
            break

        offset += limit


def get_fields(company_slug: str, data_source: Optional[str] = None, *, limit: int = 100) -> Iterable[PanoField]:
    """Get all models from remote."""
    client = FieldClient()
    offset = 0
    while True:
        try:
            fields = client.get_fields(company_slug=company_slug, data_source=data_source, offset=offset, limit=limit)
        except RequestException as e:
            if e.response is not None and e.response.status_code == requests.codes.not_found:
                raise CompanyNotFoundException(company_slug).extract_request_id(e)
            raise FieldReadException(company_slug, data_source).extract_request_id(e)

        yield from (map_field_from_remote(field) for field in fields)
        if len(fields) < limit:
            # last page
            break

        offset += limit


def get_state(company_slug: str, target_dataset: Optional[str] = None) -> VirtualState:
    """Build a representation of what VDS and models are on remote."""
    data_source_iterator = get_data_sources(company_slug)
    data_sources = (
        # Filter only targeted data source if the argument is set
        [vds for vds in data_source_iterator if vds.dataset_slug == target_dataset]
        if target_dataset
        else list(data_source_iterator)
    )
    models = list(
        itertools.chain.from_iterable(get_models(source.dataset_slug, company_slug) for source in data_sources)
    )
    fields = list(get_fields(company_slug=company_slug, data_source=target_dataset))
    return VirtualState(data_sources=data_sources, models=models, fields=fields)
