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

    @property
    def description(self) -> str:
        if self.is_deletion:
            assert self.current is not None
            return f'DELETE: {".".join(self.current.id)}'
        elif self.is_creation:
            assert self.desired is not None
            return f'CREATE: {".".join(self.desired.id)}'
        else:
            assert self.desired is not None
            return f'UPDATE: {".".join(self.desired.id)}'


class ActionList(Generic[T]):

    """Container for actions."""

    actions: List[Action[T]]

    def __init__(self, *, actions: List[Action[T]]):
        self.actions = actions

    @property
    def is_empty(self) -> bool:
        return len(self.actions) == 0

    @property
    def count(self) -> int:
        return len(self.actions)


class VirtualState:

    """Represent collection of virtual data sources."""

    data_sources: List[PanoVirtualDataSource]
    models: List[PanoModel]

    def __init__(self, *, data_sources: List[PanoVirtualDataSource], models: List[PanoModel]):
        self.data_sources = data_sources
        self.models = models

    @property
    def is_empty(self) -> bool:
        return len(self.data_sources) == 0 and len(self.models) == 0
