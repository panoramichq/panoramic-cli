from pathlib import Path
from panoramic.cli.errors import MissingContextFileException
from panoramic.cli.util import get_yaml_value


def _get_context_yaml_value(file_path: str, value_path: str):
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
