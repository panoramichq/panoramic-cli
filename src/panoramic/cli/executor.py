from abc import ABC
from typing import List, Tuple

from panoramic.cli.state import Action, ActionList, Origin


class Executor(ABC):

    """Base executor class."""

    def execute(self, action: Action):
        raise NotImplementedError('execute not implemented')


class LocalExecutor(Executor):

    """Executes actions against local filesystem."""

    def execute(self, action: Action):
        pass


class RemoteExecutor(Executor):

    """Executes actions against remote."""

    def execute(self, action: Action):
        raise NotImplementedError('execute not implemented')


def get_executor_for_direction(direction: Tuple[Origin, Origin]) -> Executor:
    return RemoteExecutor() if direction[1] == Origin.REMOTE else LocalExecutor()


def execute(actions: ActionList) -> Tuple[List[Action], List[Action]]:
    """Execute actions and return successful and failed actions."""
    executor = get_executor_for_direction(actions.direction)
    for action in actions.actions:
        executor.execute(action)

    return [], []
