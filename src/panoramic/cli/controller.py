import itertools
from typing import Iterable, Iterator

from panoramic.cli.state import Action, ActionList, VirtualState


def reconcile_data_sources(current_state: VirtualState, desired_state: VirtualState) -> Iterator[Action]:
    """Create actions that get us from current to desired state."""
    current_by_id = {s.id: s for s in current_state.data_sources}
    desired_by_id = {s.id: s for s in desired_state.data_sources}

    # delete what is not desired but exists
    ids_to_delete = current_by_id.keys() - desired_by_id.keys()
    # create what is desired but doesn't exist
    ids_to_create = desired_by_id.keys() - current_by_id.keys()
    # update what is desired and exists
    ids_to_update = current_by_id.keys() & desired_by_id.keys()

    for item_id in ids_to_delete:
        yield Action(current=current_by_id[item_id])

    for item_id in ids_to_create:
        yield Action(desired=desired_by_id[item_id])

    for item_id in ids_to_update:
        if current_by_id[item_id] != desired_by_id[item_id]:
            # only yield update actions that change state
            yield Action(current=current_by_id[item_id], desired=desired_by_id[item_id])


def reconcile_models(current_state: VirtualState, desired_state: VirtualState) -> Iterable[Action]:
    """Create actions that get us from current to desired state."""
    current_by_id = {s.id: s for s in current_state.models}
    desired_by_id = {s.id: s for s in desired_state.models}

    # delete what is not desired but exists
    ids_to_delete = current_by_id.keys() - desired_by_id.keys()
    # create what is desired but doesn't exist
    ids_to_create = desired_by_id.keys() - current_by_id.keys()
    # update what is desired and exists
    ids_to_update = current_by_id.keys() & desired_by_id.keys()

    for item_id in ids_to_delete:
        yield Action(current=current_by_id[item_id])

    for item_id in ids_to_create:
        yield Action(desired=desired_by_id[item_id])

    for item_id in ids_to_update:
        if current_by_id[item_id] != desired_by_id[item_id]:
            yield Action(current=current_by_id[item_id], desired=desired_by_id[item_id])


def reconcile(current_state: VirtualState, desired_state: VirtualState) -> ActionList:
    """Create actions that get us from current state to desired state."""
    actions = list(
        itertools.chain(
            reconcile_data_sources(current_state, desired_state), reconcile_models(current_state, desired_state)
        )
    )
    return ActionList(actions=actions)
