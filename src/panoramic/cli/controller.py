from typing import Iterable

from panoramic.cli.state import Action, ActionList, VirtualState


def _reconcile(current_state: VirtualState, desired_state: VirtualState) -> Iterable[Action]:
    current_data_sources_by_id = {s.id: s for s in current_state.data_sources}
    desired_data_sources_by_id = {s.id: s for s in desired_state.data_sources}

    # delete what is not desired but exists
    ids_to_delete = current_data_sources_by_id.keys() - desired_data_sources_by_id.keys()
    # create what is desired but doesn't exist
    ids_to_create = desired_data_sources_by_id.keys() - current_data_sources_by_id.keys()
    # update what is desired and exists
    ids_to_update = current_data_sources_by_id.keys() & desired_data_sources_by_id.keys()

    for id_ in ids_to_delete:
        yield Action.with_current(current_data_sources_by_id[id_])

    for id_ in ids_to_create:
        yield Action.with_desired(desired_data_sources_by_id[id_])

    for id_ in ids_to_update:
        yield Action(current=current_data_sources_by_id[id_], desired=desired_data_sources_by_id[id_])


def reconcile(current_state: VirtualState, desired_state: VirtualState) -> ActionList:
    """Create actions that get us from current state to desired state."""
    direction = (current_state.origin, desired_state.origin)
    actions = list(_reconcile(current_state, desired_state))
    return ActionList(actions=actions, direction=direction)
