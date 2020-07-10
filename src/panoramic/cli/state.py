from enum import Enum
from typing import Generic, List, Optional, Tuple, TypeVar

from panoramic.cli.pano_model import Actionable, PanoDataSource


class Origin(Enum):

    """Identifies whether state is remote or local."""

    LOCAL = 'LOCAL'
    REMOTE = 'REMOTE'


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

    """Container for actions."""

    actions: List[Action[T]]
    direction: Tuple[Origin, Origin]


class VirtualState:

    """Represent collection of virtual data sources."""

    data_sources: List[PanoDataSource]
    origin: Origin

    def __init__(self, *, data_sources: List[PanoDataSource], origin: Origin):
        self.data_sources = data_sources
        self.origin = origin

    @staticmethod
    def remote(*, data_sources: List[PanoDataSource]):
        return VirtualState(data_sources=data_sources, origin=Origin.REMOTE)

    @staticmethod
    def local(*, data_sources: List[PanoDataSource]):
        return VirtualState(data_sources=data_sources, origin=Origin.LOCAL)
