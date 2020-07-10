import itertools
from typing import Iterable

from panoramic.cli.pano_model import PanoDataSource, PanoModel
from panoramic.cli.state import VirtualState


def get_models(data_source: str, company_name: str) -> Iterable[PanoModel]:  # type: ignore
    """Get all models from local filesystem."""
    return []


def get_data_sources(company_name: str) -> Iterable[PanoDataSource]:  # type: ignore
    """Get virtual data sources from local filesystem."""
    return []


def get_state(company_name: str) -> VirtualState:
    """Build a representation of what VDS and models are on local filesystem."""
    data_sources = list(get_data_sources(company_name))
    models = list(itertools.chain.from_iterable(get_models(source.slug, company_name) for source in data_sources))
    return VirtualState.local(data_sources=data_sources, models=models)
