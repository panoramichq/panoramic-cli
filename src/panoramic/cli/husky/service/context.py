from typing import Optional, Union

from sqlalchemy.engine import default

from panoramic.cli.connection import Connection
from panoramic.cli.husky.common.enum import EnumHelper
from panoramic.cli.husky.service.helpers import RUNTIME_DIALECTS
from panoramic.cli.husky.service.types.api_data_request_types import (
    BlendingDataRequest,
    InternalDataRequest,
)
from panoramic.cli.husky.service.types.enums import HuskyQueryRuntime
from panoramic.cli.husky.service.utils.exceptions import UnsupportedSQLOutputException


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

    @classmethod
    def from_request(
        cls,
        data_request: Union[BlendingDataRequest, InternalDataRequest],
        default_runtime: Optional[HuskyQueryRuntime] = None,
    ):
        if default_runtime:
            return cls(default_runtime)
        connection = Connection.get()

        query_runtime_name = Connection.get_dialect_name(connection)
        query_runtime = EnumHelper.from_value_safe(HuskyQueryRuntime, query_runtime_name)
        if query_runtime is None:
            raise UnsupportedSQLOutputException(query_runtime_name)

        return cls(query_runtime)


SNOWFLAKE_HUSKY_CONTEXT = HuskyQueryContext(HuskyQueryRuntime.snowflake)
