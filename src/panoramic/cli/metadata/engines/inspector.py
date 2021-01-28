from sqlalchemy.engine.reflection import Inspector

from panoramic.cli.connections import Connections
from panoramic.cli.metadata.engines.base import BaseScanner


class InspectorScanner(BaseScanner):
    """Metadata scanner using SQLAlchemy inspector"""

    def scan(self, *, force_reset: bool = False):
        """Scan Snowflake storage"""
        connection = self._get_connection()

        engine = Connections.get_connection_engine(connection)
        Inspector.from_engine(engine)
        # list all available tables
        # print(inspector.sorted_tables)
