from typing import List, Optional

from panoramic.cli.models import get_local as get_local_models
from panoramic.cli.models import get_remote as get_remote_models
from panoramic.cli.virtualdatasource import get_local as get_local_datasources
from panoramic.cli.virtualdatasource import get_remote as get_remote_datasources


class Action:

    """Action that transitions state from current to desired state."""


class Model:

    """TODO: add docstring"""


class DataSource:

    """TODO: add docstring"""

    models: List[Model]

    def __init__(self, *, models: Optional[List[Model]] = None):
        if models is None:
            models = []

        self.models = models


class VirtualState:

    """TODO: add docstring"""

    data_sources: List[DataSource]

    def __init__(self, *, data_sources: Optional[List[DataSource]] = None):
        if data_sources is None:
            data_sources = []

        self.data_sources = data_sources

    def add(self, data_source: DataSource):
        self.data_sources.append(data_source)


def get_remote(company_name: str) -> VirtualState:
    """Build a representation of what VDS and models are on remote."""
    virtual_sources = get_remote_datasources(company_name)
    data_sources = [DataSource(models=list(get_remote_models(source.name, company_name))) for source in virtual_sources]
    return VirtualState(data_sources=data_sources)


def get_local(company_name: str) -> VirtualState:
    """Build a representation of what VDS and models are on local filesystem."""
    virtual_sources = get_local_datasources(company_name)
    data_sources = [DataSource(models=list(get_local_models(source.name, company_name))) for source in virtual_sources]
    return VirtualState(data_sources=data_sources)
