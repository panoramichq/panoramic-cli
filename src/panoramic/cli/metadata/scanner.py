from typing import Type

from panoramic.cli.husky.service.types.enums import HuskyQueryRuntime
from panoramic.cli.metadata.engines.base import BaseScanner
from panoramic.cli.metadata.engines.inspector import InspectorScanner
from panoramic.cli.metadata.engines.snowflake import SnowflakeScanner


class Scanner:
    @staticmethod
    def get_scanner(connection_dialect: HuskyQueryRuntime) -> Type[BaseScanner]:
        """Determines which scanner use for the connection dialect"""

        if connection_dialect == HuskyQueryRuntime.snowflake:
            # use specific Snowflake scanner to work with multiple databases
            return SnowflakeScanner
        else:
            # scanner using SQLAlchemy is a sensible default for other options
            return InspectorScanner
