from typing import List

from panoramic.cli.state import Action, VirtualState


def reconcile(current_state: VirtualState, desired_state: VirtualState) -> List[Action]:
    """Create actions that get us from current state to desired state."""
    pass
