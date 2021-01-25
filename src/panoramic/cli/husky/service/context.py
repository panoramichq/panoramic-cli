from typing import Union

from sqlalchemy.engine import default

from panoramic.cli.connections import Connections
from panoramic.cli.husky.common.enum import EnumHelper
from panoramic.cli.husky.service.helpers import RUNTIME_DIALECTS
from panoramic.cli.husky.service.types.api_data_request_types import (
    BlendingDataRequest,
    InternalDataRequest,
)
from panoramic.cli.husky.service.types.enums import HuskyQueryRuntime
from panoramic.cli.husky.service.utils.exceptions import (
    TooManyPhysicalDataSourcesException,
    UnsupportedSQLOutputException,
)


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
    def from_request(cls, data_request: Union[BlendingDataRequest, InternalDataRequest]):
        if data_request.physical_data_sources:
            if len(data_request.physical_data_sources) == 1:
                request_pds = data_request.physical_data_sources[0]
                connection = Connections.get_by_name(request_pds, True)

                query_runtime_name = Connections.get_connection_engine(connection).dialect.name
                query_runtime = EnumHelper.from_value_safe(HuskyQueryRuntime, query_runtime_name)
                if query_runtime is None:
                    raise UnsupportedSQLOutputException(query_runtime_name)

                return cls(query_runtime)
            elif len(data_request.physical_data_sources) > 1:
                raise TooManyPhysicalDataSourcesException(data_request.physical_data_sources)
        else:
            return cls(HuskyQueryRuntime.snowflake)


SNOWFLAKE_HUSKY_CONTEXT = HuskyQueryContext(HuskyQueryRuntime.snowflake)
