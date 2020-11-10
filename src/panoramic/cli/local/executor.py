from typing import Optional

from panoramic.cli.executor import Executor
from panoramic.cli.local.writer import FileWriter
from panoramic.cli.state import Action


class LocalExecutor(Executor):

    """Executes actions against local filesystem."""

    writer: FileWriter

    def __init__(self, writer: Optional[FileWriter] = None):
        if writer is None:
            writer = FileWriter()
        self.writer = writer

    def _execute(self, action: Action):
        if action.is_creation:
            assert action.desired is not None
            self.writer.write(action.desired)
        elif action.is_deletion:
            assert action.current is not None
            self.writer.delete(action.current)
        else:
            # assume update
            assert action.desired is not None
            assert action.current is not None
            self.writer.write(action.desired, package=action.current.package, file_name=action.current.file_name)
