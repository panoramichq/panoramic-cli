import logging
import random
from typing import Any, Dict, List, Optional

from core.dremio.client import HuskyDremioClient
from core.sql_alchemy_util import compile_query
from federated.identifier_parser.configuration import Configuration, ParameterStore
from husky.helpers import RUNTIME_DIALECTS
from husky.types.enums import HuskyQueryRuntime
from husky.util.dremio.dialect import DremioDialect
from sqlalchemy import column, select, table
from sqlalchemy.sql.compiler import IdentifierPreparer

logger = logging.getLogger(__name__)


class PhysicalDataSourceSampler:
    def __init__(self, params: Optional[Configuration]):
        self.params = params if params is not None else ParameterStore().get_config()

    def _build_sample_query(self, physical_data_source: str, table_name: str, columns: List[str]) -> str:
        preparer = IdentifierPreparer(DremioDialect())
        [*maybe_db_and_schema, only_table_name] = preparer.unformat_identifiers(table_name)

        source_table = table(preparer.quote_identifier(only_table_name))
        source_table.schema = '.'.join(
            [preparer.quote_identifier(physical_data_source)]
            + [preparer.quote_identifier(component) for component in maybe_db_and_schema]
        )

        query = select([column(c) for c in columns]).select_from(source_table).limit(self.params.population_size)

        return compile_query(query, RUNTIME_DIALECTS[HuskyQueryRuntime.dremio])

    def sample(self, physical_data_source: str, table_name: str, columns: List[str]) -> List[Dict[str, Any]]:
        with HuskyDremioClient() as dremio_client:
            _dremio_job_id, _row_count, data = dremio_client.execute_and_get_data(
                self._build_sample_query(physical_data_source, table_name, columns)
            )

            data_list = list(data)
            if len(data_list) <= self.params.sample_size:
                return data_list

            return random.sample(data_list, self.params.sample_size)
