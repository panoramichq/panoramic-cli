from pathlib import Path

from click.core import Command, Context

from panoramic.cli.errors import CriticalError, MissingContextFileException
from panoramic.cli.util import get_yaml_value


class ContextAwareCommand(Command):

    """Perform context file check before running command."""

    def invoke(self, ctx: Context):
        if not (Path.cwd() / 'pano.yaml').exists():
            raise CriticalError('Context file (pano.yaml) not found in working directory.')
        return super().invoke(ctx)


def _get_context_yaml_value(file_path: Path, value_path: str):
    try:
        return get_yaml_value(file_path, value_path)
    except FileNotFoundError:
        raise MissingContextFileException()


def get_api_version() -> str:
    """Return api version from context."""
    return _get_context_yaml_value(Path.cwd() / 'pano.yaml', 'api_version')


def get_company_slug() -> str:
    """Return company slug from context."""
    return _get_context_yaml_value(Path.cwd() / 'pano.yaml', 'company_slug')
