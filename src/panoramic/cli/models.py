from typing import Any, Dict, Iterable

from panoramic.cli.model.client import ModelClient


def get_remote(data_source: str, company_name: str, limit: int = 100) -> Iterable[Dict[str, Any]]:
    """Get all models from remote."""
    client = ModelClient()
    offset = 0
    while True:
        models = client.get_models(data_source, company_name, offset=offset, limit=limit)
        yield from models
        if len(models) < limit:
            # last page
            break


def get_local(data_source: str, company_name: str) -> Iterable[Dict[str, Any]]:
    """Get all models from local filesystem."""
    pass
