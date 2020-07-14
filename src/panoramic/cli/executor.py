from abc import ABC

from panoramic.cli.state import Action


class Executor(ABC):

    """Base executor class."""

    def execute(self, action: Action):
        raise NotImplementedError('execute not implemented in base class')
