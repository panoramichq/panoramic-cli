import logging
from dataclasses import dataclass

from dbt.exceptions import FailedToConnectException
from requests import RequestException

from panoramic.cli.connections import (
    CONNECTION_KEYS,
    Connections,
    get_dialect_credentials,
)
from panoramic.cli.errors import (
    ConnectionNotFound,
    TransformCompileException,
    TransformExecutionFailed,
)
from panoramic.cli.print import echo_info
from panoramic.cli.transform.client import TransformClient
from panoramic.cli.transform.pano_transform import PanoTransform

logger = logging.getLogger(__name__)


@dataclass
class TransformExecutor:

    company_slug: str
    transform: PanoTransform
    compiled_query: str

    @classmethod
    def from_transform(cls, company_slug: str, transform: PanoTransform) -> 'TransformExecutor':
        try:
            transform_client = TransformClient()
            transform_executor = cls(company_slug=company_slug, transform=transform, compiled_query='')

            compiled_query = transform_client.compile_transform(transform, company_slug, transform.connection_name)
            create_view_statement = f"CREATE OR REPLACE VIEW {transform.view_path} AS ({compiled_query})"

            transform_executor.compiled_query = create_view_statement
            return transform_executor
        except RequestException as request_exception:
            raise TransformCompileException(transform.name).extract_request_id(request_exception)

    def execute(self):
        connection_name = self.transform.connection_name
        try:
            connection = Connections.get_by_name(connection_name)
        except ValueError:
            raise ConnectionNotFound(connection_name)

        for key in CONNECTION_KEYS:
            if key not in connection:
                connection[key] = ''

        credentials, connection_error = get_dialect_credentials(connection)

        if connection_error is not None:
            # FIXME: raise a better error
            echo_info(f'{connection_name} FAIL: {connection_error}...')
            return

        try:
            logger.debug(f'Executing transform {self.transform.name} on {self.transform.connection_name}')
            Connections.execute(sql=self.compiled_query, credentials=credentials)

            logger.debug(f'Verifying transform {self.transform.name} on {self.transform.connection_name}')
            Connections.execute(sql=f'SELECT * from {self.transform.view_path} limit 1', credentials=credentials)
        except FailedToConnectException:
            raise
        except Exception:
            raise TransformExecutionFailed(
                transform_name=self.transform.name, connection_name=connection_name, compiled_sql=self.compiled_query
            )
