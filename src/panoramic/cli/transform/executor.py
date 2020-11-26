from dataclasses import dataclass

from requests import RequestException

from panoramic.cli.connections import Connections, get_dialect_credentials
from panoramic.cli.errors import ConnectionNotFound, TransformCompileException
from panoramic.cli.print import echo_info
from panoramic.cli.transform.client import TransformClient
from panoramic.cli.transform.pano_transform import PanoTransform


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

            compiled_query = transform_client.compile_transform(transform, company_slug)
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

        credentials, connection_error = get_dialect_credentials(connection)

        if connection_error is not None:
            # FIXME: raise a better error
            echo_info(f'{connection_name} FAIL: {connection_error}...')
            return

        Connections.execute(sql=self.compiled_query, credentials=credentials)

        # Check if the view works
        # Connections.execute(sql=f'SELECT * from {schema_view} LIMIT 1')
