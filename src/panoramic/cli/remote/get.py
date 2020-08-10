import itertools
from typing import Iterable

from requests.exceptions import RequestException

from panoramic.cli.errors import ModelException, VirtualDataSourceException
from panoramic.cli.mapper import map_data_source_from_remote, map_model_from_remote
from panoramic.cli.model import ModelClient
from panoramic.cli.pano_model import PanoModel, PanoVirtualDataSource
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
            raise VirtualDataSourceException(company_slug).extract_request_id(e)

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
            raise ModelException(company_slug, data_source).extract_request_id(e)

        yield from (map_model_from_remote(m) for m in models)
        if len(models) < limit:
            # last page
            break

        offset += limit


def get_state(company_slug: str) -> VirtualState:
    """Build a representation of what VDS and models are on remote."""
    data_sources = list(get_data_sources(company_slug))
    models = list(
        itertools.chain.from_iterable(get_models(source.dataset_slug, company_slug) for source in data_sources)
    )
    return VirtualState(data_sources=data_sources, models=models)
