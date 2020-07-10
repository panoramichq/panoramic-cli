from typing import Iterable, Iterator

from panoramic.cli.state import Action, ActionList, VirtualState


def reconcile_data_sources(current_state: VirtualState, desired_state: VirtualState) -> Iterator[Action]:
    """Create actions that get us from current to desired state."""
    current_by_id = {s.data_source_slug: s for s in current_state.data_sources}
    desired_by_id = {s.data_source_slug: s for s in desired_state.data_sources}

    # delete what is not desired but exists
    ids_to_delete = current_by_id.keys() - desired_by_id.keys()
    # create what is desired but doesn't exist
    ids_to_create = desired_by_id.keys() - current_by_id.keys()
    # update what is desired and exists
    ids_to_update = current_by_id.keys() & desired_by_id.keys()

    for id_ in ids_to_delete:
        yield Action(current=current_by_id[id_])

    for id_ in ids_to_create:
        yield Action(desired=desired_by_id[id_])

    for id_ in ids_to_update:
        yield Action(current=current_by_id[id_], desired=desired_by_id[id_])


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

    for id_ in ids_to_delete:
        yield Action(current=current_by_id[id_])

    for id_ in ids_to_create:
        yield Action(desired=desired_by_id[id_])

    for id_ in ids_to_update:
        yield Action(current=current_by_id[id_], desired=desired_by_id[id_])


def _reconcile(current_state: VirtualState, desired_state: VirtualState) -> Iterable[Action]:
    yield from reconcile_data_sources(current_state, desired_state)
    yield from reconcile_models(current_state, desired_state)


def reconcile(current_state: VirtualState, desired_state: VirtualState) -> ActionList:
    """Create actions that get us from current state to desired state."""
    direction = (current_state.origin, desired_state.origin)
    actions = list(_reconcile(current_state, desired_state))
    return ActionList(actions=actions, direction=direction)
