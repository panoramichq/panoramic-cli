from sqlalchemy.engine.reflection import Inspector

from panoramic.cli.connection import Connection
from panoramic.cli.metadata.engines.with_connection import WithConnection


class InspectorScanner(WithConnection):
    """Metadata scanner using SQLAlchemy inspector"""

    def scan(self, *, force_reset: bool = False):
        """Scan Snowflake storage"""
        connection = self._get_connection()

        engine = Connection.get_connection_engine(connection)
        Inspector.from_engine(engine)
        # list all available tables
        # print(inspector.sorted_tables)
