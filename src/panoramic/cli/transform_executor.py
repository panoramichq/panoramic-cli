from requests import RequestException

from panoramic.cli.connections import Connections, get_dialect_credentials
from panoramic.cli.errors import ConnectionNotFound, TransformCompileException
from panoramic.cli.print import echo_info
from panoramic.cli.transform.client import TransformClient
from panoramic.cli.transform.pano_transform import PanoTransform


class TransformExecutor:
    @classmethod
    def execute(cls, transform: PanoTransform, query: str):
        connection_name, schema_view = transform.target.split('.', 1)
        try:
            connection = Connections.get_by_name(connection_name)
        except ValueError:
            raise ConnectionNotFound(connection_name)

        credentials, connection_error = get_dialect_credentials(connection)

        if connection_error is not None:
            # FIXME: raise a better error
            echo_info(f'{connection_name} FAIL: {connection_error}...')
            return

        Connections.execute(sql=query, credentials=credentials)

    @classmethod
    def compile(cls, transform: PanoTransform, company_slug: str) -> str:
        try:
            transform_client = TransformClient()
            compiled_query = transform_client.compile_transform(transform, company_slug)
            create_view_statement = f"CREATE VIEW {transform.target} AS {compiled_query}"
            return create_view_statement
        except RequestException as request_exception:
            raise TransformCompileException(transform.name).extract_request_id(request_exception)
