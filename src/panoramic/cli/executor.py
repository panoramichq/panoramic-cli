from abc import ABC
from typing import List, Tuple

from panoramic.cli.local.writer import FileWriter
from panoramic.cli.state import Action, ActionList, Origin


class Executor(ABC):

    """Base executor class."""

    def execute(self, action: Action):
        raise NotImplementedError('execute not implemented')


class LocalExecutor(Executor):

    """Executes actions against local filesystem."""

    def __init__(self, writer: FileWriter):
        self.writer = writer

    def execute(self, action: Action):
        if action.is_creation:
            assert action.desired is not None
            self.writer.write(action.desired)
        elif action.is_deletion:
            assert action.current is not None
            self.writer.delete(action.current)
        else:
            # assume update
            assert action.desired is not None
            self.writer.write(action.desired)


class RemoteExecutor(Executor):

    """Executes actions against remote."""

    def execute(self, action: Action):
        raise NotImplementedError('execute not implemented')


def get_executor_for_direction(direction: Tuple[Origin, Origin]) -> Executor:
    return RemoteExecutor() if direction[1] == Origin.REMOTE else LocalExecutor(FileWriter())


def execute(actions: ActionList) -> Tuple[List[Action], List[Action]]:
    """Execute actions and return successful and failed actions."""
    executor = get_executor_for_direction(actions.direction)
    for action in actions.actions:
        executor.execute(action)

    return [], []
