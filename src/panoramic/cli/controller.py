import itertools
from typing import Iterable, Sequence

from panoramic.cli.pano_model import Actionable
from panoramic.cli.state import Action, ActionList, VirtualState


def _reconcile_actions(
    current_entities: Sequence[Actionable], desired_entities: Sequence[Actionable]
) -> Iterable[Action]:
    """Create actions that get us from current to desired state."""
    current_by_id = {s.id: s for s in current_entities}
    desired_by_id = {s.id: s for s in desired_entities}

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
            _reconcile_actions(current_state.data_sources, desired_state.data_sources),
            _reconcile_actions(current_state.models, desired_state.models),
            _reconcile_actions(current_state.fields, desired_state.fields),
        )
    )
    return ActionList(actions=actions)
