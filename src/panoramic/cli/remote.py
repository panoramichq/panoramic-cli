from typing import Iterable

from panoramic.cli.model import ModelClient
from panoramic.cli.pano_model import PanoDataSource, PanoModel
from panoramic.cli.state import VirtualState

# from panoramic.cli.virtual_data_source import VirtualDataSourceClient


def get_data_sources(company_name: str) -> Iterable[PanoModel]:
    """Get virtual data sources from remote."""
    # client = VirtualDataSourceClient()
    # offset = 0
    # while True:
    #     sources = client.get_virtual_data_sources(company_name, offset=offset, limit=limit)
    #     yield from sources
    #     if len(models) < limit:
    #         # last page
    #         break
    #     offset += limit


def get_models(data_source: str, company_name: str, limit: int = 100) -> Iterable[PanoModel]:
    """Get all models from remote."""
    client = ModelClient()
    offset = 0
    while True:
        models = client.get_models(data_source, company_name, offset=offset, limit=limit)
        yield from (PanoModel.from_dict(d) for d in models)
        if len(models) < limit:
            # last page
            break

        offset += limit


def get_state(company_name: str) -> VirtualState:
    """Build a representation of what VDS and models are on remote."""
    virtual_sources = get_data_sources(company_name)
    data_sources = [PanoDataSource(models=list(get_models(source.name, company_name))) for source in virtual_sources]
    return VirtualState.remote(data_sources=data_sources)
