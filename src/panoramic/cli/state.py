from collections import defaultdict
from typing import Dict, Generic, List, Optional, Tuple, TypeVar

from panoramic.cli.pano_model import (
    Actionable,
    PanoField,
    PanoModel,
    PanoVirtualDataSource,
)

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

    def __hash__(self) -> int:
        return hash(
            (
                self.current,
                self.desired,
            )
        )

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, Action):
            return False

        return (self.current, self.desired) == (o.current, o.desired)


class ActionList(Generic[T]):
    """Container for actions."""

    actions: List[Action[T]]

    def __init__(self, *, actions: Optional[List[Action[T]]] = None):
        self.actions = actions if actions is not None else []

    @property
    def is_empty(self) -> bool:
        return len(self.actions) == 0

    @property
    def count(self) -> int:
        return len(self.actions)

    def add_action(self, action: Action):
        self.actions.append(action)

    def __hash__(self) -> int:
        return hash(tuple(self.actions))

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, ActionList):
            return False

        return self.actions == o.actions


class VirtualState:
    """Represent collection of virtual data sources."""

    data_sources: List[PanoVirtualDataSource]
    models: List[PanoModel]
    fields: List[PanoField]

    def __init__(self, *, data_sources: List[PanoVirtualDataSource], models: List[PanoModel], fields: List[PanoField]):
        self.data_sources = data_sources
        self.models = models
        self.fields = fields

    @property
    def is_empty(self) -> bool:
        return len(self.data_sources) == 0 and len(self.models) == 0 and len(self.fields) == 0

    def get_objects_by_package(self) -> Dict[str, Tuple[List[PanoField], List[PanoModel]]]:
        objects_by_dataset: Dict[str, Tuple[List[PanoField], List[PanoModel]]] = defaultdict(lambda: ([], []))

        for field in self.fields:
            # exclude global fields
            if field.package is not None:
                objects_by_dataset[field.package][0].append(field)

        for model in self.models:
            # models always have a package
            assert model.package is not None
            objects_by_dataset[model.package][1].append(model)

        return dict(objects_by_dataset)
