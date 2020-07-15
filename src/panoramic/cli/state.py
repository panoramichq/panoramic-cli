from typing import Generic, List, Optional, TypeVar

from panoramic.cli.pano_model import Actionable, PanoModel, PanoVirtualDataSource

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
    def is_creation(self) -> bool:
        return self.current is None

    @property
    def is_deletion(self) -> bool:
        return self.desired is None


class ActionList(Generic[T]):

    """Container for actions."""

    actions: List[Action[T]]

    def __init__(self, *, actions: List[Action[T]]):
        self.actions = actions


class VirtualState:

    """Represent collection of virtual data sources."""

    data_sources: List[PanoVirtualDataSource]
    models: List[PanoModel]

    def __init__(self, *, data_sources: List[PanoVirtualDataSource], models: List[PanoModel]):
        self.data_sources = data_sources
        self.models = models
