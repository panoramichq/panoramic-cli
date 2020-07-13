import logging

from typing import Optional
from panoramic.cli.executor import Executor
from panoramic.cli.remote.writer import RemoteWriter
from panoramic.cli.state import Action

logger = logging.getLogger(__name__)


class RemoteExecutor(Executor):

    """Executes actions against remote API."""

    company_name: str
    writer: RemoteWriter

    def __init__(self, company_name: str, *, writer: Optional[RemoteWriter] = None):
        self.company_name = company_name

        if writer is None:
            writer = RemoteWriter(company_name)

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
