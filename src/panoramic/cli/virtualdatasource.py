from typing import Any, Dict, Iterable


def get_remote(company_name: str) -> Iterable[Dict[str, Any]]:
    """Get virtual data sources from remote."""
    # TODO: Implement once VDS Client ready to use
    # client = VirtualDataSourceClient()
    # offset = 0
    # while True:
    #     sources = client.get_virtual_data_sources(company_name, offset=offset, limit=limit)
    #     yield from sources
    #     if len(models) < limit:
    #         # last page
    #         break


def get_local(company_name: str) -> Iterable[Dict[str, Any]]:
    """Get virtual data sources from local filesystem."""
    pass
