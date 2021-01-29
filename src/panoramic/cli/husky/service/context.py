from sqlalchemy.engine import default

from panoramic.cli.husky.service.helpers import RUNTIME_DIALECTS
from panoramic.cli.husky.service.types.enums import HuskyQueryRuntime


class HuskyQueryContext:
    _query_runtime: HuskyQueryRuntime

    def __init__(self, query_runtime: HuskyQueryRuntime):
        self._query_runtime = query_runtime

    @property
    def query_runtime(self) -> HuskyQueryRuntime:
        return self._query_runtime

    @property
    def dialect(self) -> default.DefaultDialect:
        return RUNTIME_DIALECTS[self._query_runtime]


SNOWFLAKE_HUSKY_CONTEXT = HuskyQueryContext(HuskyQueryRuntime.snowflake)
