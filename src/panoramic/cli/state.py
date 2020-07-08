from abc import ABC
from enum import Enum
from typing import Generic, List, Optional, Tuple, TypeVar

from panoramic.cli.models import get_local as get_local_models
from panoramic.cli.models import get_remote as get_remote_models
from panoramic.cli.virtualdatasource import get_local as get_local_datasources
from panoramic.cli.virtualdatasource import get_remote as get_remote_datasources


class Origin(Enum):

    """Identifies whether state is remote or local."""

    LOCAL = 'LOCAL'
    REMOTE = 'REMOTE'


class Actionable(ABC):

    """Interface for object that you can perform actions on."""


T = TypeVar('T', bound=Actionable)


class Action(Generic[T]):

    """Action that transitions state from current to desired."""

    current: Optional[T]
    desired: Optional[T]

    def __init__(self, *, current: Optional[T] = None, desired: Optional[T] = None):
        if current is None and desired is None:
            raise ValueError('Both current and desired cannot be None')

        self.current = current
        self.desired = desired

    @staticmethod
    def with_desired(desired: T):
        return Action(desired=desired)

    @staticmethod
    def with_current(current: T):
        return Action(current=current)


class ActionList(Generic[T]):

    actions: List[Action[T]]
    direction: Tuple[Origin, Origin]


class Model(Actionable):

    """TODO: add docstring"""

    # TODO: add attrs once https://github.com/panoramichq/pano-cli/pull/4 is ready


class VirtualDataSource(Actionable):

    """TODO: add docstring"""

    # TODO: add attrs once https://github.com/panoramichq/pano-cli/pull/4 is ready

    models: List[Model]

    def __init__(self, *, models: List[Model]):
        self.models = models


class VirtualState:

    """TODO: add docstring"""

    data_sources: List[VirtualDataSource]
    origin: Origin

    def __init__(self, *, data_sources: List[VirtualDataSource], origin: Origin):
        self.data_sources = data_sources
        self.origin = origin

    @staticmethod
    def remote(*, data_sources: List[VirtualDataSource]):
        return VirtualState(data_sources=data_sources, origin=Origin.REMOTE)

    @staticmethod
    def local(*, data_sources: List[VirtualDataSource]):
        return VirtualState(data_sources=data_sources, origin=Origin.LOCAL)


def get_remote(company_name: str) -> VirtualState:
    """Build a representation of what VDS and models are on remote."""
    # For now hold everything in memory
    virtual_sources = get_remote_datasources(company_name)
    data_sources = [
        VirtualDataSource(models=list(get_remote_models(source.name, company_name))) for source in virtual_sources
    ]
    return VirtualState.remote(data_sources=data_sources)


def get_local(company_name: str) -> VirtualState:
    """Build a representation of what VDS and models are on local filesystem."""
    # For now hold everything in memory
    virtual_sources = get_local_datasources(company_name)
    data_sources = [
        VirtualDataSource(models=list(get_local_models(source.name, company_name))) for source in virtual_sources
    ]
    return VirtualState.local(data_sources=data_sources)
