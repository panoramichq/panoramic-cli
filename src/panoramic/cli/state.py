from enum import Enum
from typing import Generic, List, Optional, Tuple, TypeVar

from panoramic.cli.pano_model import Actionable, PanoDataSource, PanoModel


class Origin(Enum):

    """Identifies whether state is remote or local."""

    LOCAL = 'LOCAL'
    REMOTE = 'REMOTE'


T = TypeVar('T', covariant=True, bound=Actionable)


class Action(Generic[T]):

    """Action that transitions state from current to desired."""

    current: Optional[T]
    desired: Optional[T]

    def __init__(self, *, current: Optional[T] = None, desired: Optional[T] = None):
        if current is None and desired is None:
            raise ValueError('Both current and desired cannot be None')

        self.current = current
        self.desired = desired

    @property
    def is_creation(self):
        return self.current is None

    @property
    def is_deletion(self):
        return self.desired is None


class ActionList(Generic[T]):

    """Container for actions."""

    actions: List[Action[T]]
    direction: Tuple[Origin, Origin]

    def __init__(self, *, actions: List[Action[T]], direction: Tuple[Origin, Origin]):
        self.actions = actions
        self.direction = direction


class VirtualState:

    """Represent collection of virtual data sources."""

    data_sources: List[PanoDataSource]
    models: List[PanoModel]
    origin: Origin

    def __init__(self, *, data_sources: List[PanoDataSource], models: List[PanoModel], origin: Origin):
        self.data_sources = data_sources
        self.models = models
        self.origin = origin

    @staticmethod
    def remote(*, data_sources: List[PanoDataSource], models: List[PanoModel]):
        return VirtualState(data_sources=data_sources, models=models, origin=Origin.REMOTE)

    @staticmethod
    def local(*, data_sources: List[PanoDataSource], models: List[PanoModel]):
        return VirtualState(data_sources=data_sources, models=models, origin=Origin.LOCAL)
