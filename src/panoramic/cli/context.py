from pathlib import Path

from click.core import Command, Context

from panoramic.cli.errors import MissingContextFileException
from panoramic.cli.local.file_utils import PresetFileName
from panoramic.cli.util import get_yaml_value


class ContextAwareCommand(Command):

    """Perform context file check before running command."""

    def invoke(self, ctx: Context):
        if not (Path.cwd() / PresetFileName.CONTEXT.value).exists():
            raise MissingContextFileException(
                f'Context file ({PresetFileName.CONTEXT.value}) not found in working directory.'
            )
        return super().invoke(ctx)


def _get_context_yaml_value(file_path: Path, value_path: str):
    try:
        return get_yaml_value(file_path, value_path)
    except FileNotFoundError:
        raise MissingContextFileException()


def get_api_version() -> str:
    """Return api version from context."""
    return _get_context_yaml_value(Path.cwd() / PresetFileName.CONTEXT.value, 'api_version')


def get_company_slug() -> str:
    """Return company slug from context."""
    return _get_context_yaml_value(Path.cwd() / PresetFileName.CONTEXT.value, 'company_slug')
