import functools
from typing import Any, Dict, List, Optional

from panoramic.cli.file_utils import read_yaml
from panoramic.cli.paths import Paths


@functools.lru_cache()
def get_company_slug() -> str:
    """Return company slug from context."""
    return read_yaml(Paths.context_file())['company_slug']


@functools.lru_cache()
def get_dbt_packages() -> Optional[List[Dict[str, Any]]]:
    """Return dbt packages from context."""
    return read_yaml(Paths.context_file()).get('dbt', {}).get('packages')


@functools.lru_cache()
def get_dbt_vars() -> Optional[Dict[str, Any]]:
    """Return dbt variables from context."""
    return read_yaml(Paths.context_file()).get('dbt', {}).get('vars')


@functools.lru_cache()
def get_dbt_target_name() -> str:
    """Return dbt target from context."""
    # target name is required if DBT is set
    return read_yaml(Paths.context_file())['dbt']['target']['name']


@functools.lru_cache()
def get_dbt_target_parts() -> Dict[str, Any]:
    """Return dbt target parts from context."""
    return read_yaml(Paths.context_file())['dbt']['target']['parts']
