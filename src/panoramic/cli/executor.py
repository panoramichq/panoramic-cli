from abc import ABC

from panoramic.cli.state import Action


class Executor(ABC):

    """Base executor class."""

    success_count: int = 0
    failure_count: int = 0
    total_count: int = 0

    def execute(self, action: Action):
        try:
            self._execute(action)
            self.success_count += 1
        except Exception:
            self.failure_count += 1
            raise
        finally:
            self.total_count += 1

    def _execute(self, action: Action):
        raise NotImplementedError('execute not implemented in base class')
