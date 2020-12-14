from requests import RequestException

from panoramic.cli.errors import TransformCompileException
from panoramic.cli.transform.client import TransformClient
from panoramic.cli.transform.pano_transform import CompiledTransform, PanoTransform


class TransformCompiler:
    @classmethod
    def compile(cls, transform: PanoTransform, company_slug: str) -> CompiledTransform:
        try:
            transform_client = TransformClient()

            compiled_query = transform_client.compile_transform(transform, company_slug, transform.connection_name)
            create_view_statement = f"CREATE OR REPLACE VIEW {transform.view_path} AS ({compiled_query})"

            return CompiledTransform(
                transform=transform, company_slug=company_slug, compiled_query=create_view_statement
            )
        except RequestException as request_exception:
            raise TransformCompileException(transform.name).extract_request_id(request_exception)
