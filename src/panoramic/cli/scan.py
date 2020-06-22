import itertools
import operator

from typing import Any, Dict, Iterable

import yaml

from panoramic.cli.metadata import MetadataClient


def columns_to_tables(columns: Iterable[Dict]) -> Iterable[Dict]:
    """Map iterable of ordered column records to tables."""
    columns_grouped = itertools.groupby(columns, operator.itemgetter('table_schema', 'table_name'))
    return (
        {'name': table_name, 'schema': table_schema, 'columns': columns,}
        for (table_schema, table_name), columns in columns_grouped
    )


class Scanner:

    """Scans metadata for a given source and scope."""

    source_id: str
    client: MetadataClient

    def __init__(self, source_id: str, client: MetadataClient = None):
        self.source_id = source_id

        if client is None:
            self.client = MetadataClient()

    def run(self, scope: str) -> Iterable[Dict[str, Any]]:
        """Perform metadata scan for given source and scope."""
        yield from columns_to_tables(self.scan_columns(scope))

    def scan_columns(self, scope: str) -> Iterable[Dict]:
        """Scan columns for a given source and scope."""
        page = 0
        limit = 100
        while True:
            columns = self.client.get_columns(self.source_id, scope, page=page, limit=limit)
            for column in columns:
                yield column

            # last page
            if len(columns) < limit:
                return

            # get next page
            page += 1
