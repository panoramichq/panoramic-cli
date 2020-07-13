from panoramic.cli.local.writer import FileWriter
from panoramic.cli.remote.writer import ApiWriter
from panoramic.cli.state import Action, ActionList


class Executor:

    """Base executor class."""


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

    """Executes actions against remote API."""

    def __init__(self, writer: ApiWriter):
        self.writer = writer

    def execute(self, action: Action, company_name: str):
        if action.is_creation:
            assert action.desired is not None
            self.writer.write(action.desired, company_name)
        elif action.is_deletion:
            assert action.current is not None
            self.writer.delete(action.current, company_name)
        else:
            # assume update
            assert action.desired is not None
            self.writer.write(action.desired, company_name)


def execute_local(actions: ActionList):
    """Execute actions and return successful and failed actions."""
    executor = LocalExecutor(FileWriter())
    for action in actions.actions:
        executor.execute(action)


def execute_remote(actions: ActionList, company_name: str):
    """Execute actions and return successful and failed actions."""
    executor = RemoteExecutor(ApiWriter())
    for action in actions.actions:
        executor.execute(action, company_name)
