from requests import RequestException

from panoramic.cli.errors import TransformCompileException
from panoramic.cli.husky.core.federated.transform.models import TransformRequest
from panoramic.cli.husky.federated.transform.service import TransformService
from panoramic.cli.transform.pano_transform import CompiledTransform, PanoTransform


class TransformCompiler:
    company_id: str

    def __init__(self, company_id: str):
        self.company_id = company_id

    def compile(self, transform: PanoTransform) -> CompiledTransform:
        try:
            transform_request = TransformRequest(fields=transform.fields, filter=transform.filters)
            compiled_query, _ = TransformService.compile_transformation_request(
                transform_request, self.company_id, transform.connection_name
            )
            create_view_statement = f"CREATE OR REPLACE VIEW {transform.view_path} AS ({compiled_query})"

            return CompiledTransform(
                transform=transform, company_id=self.company_id, compiled_query=create_view_statement
            )
        except RequestException as request_exception:
            raise TransformCompileException(transform.name).extract_request_id(request_exception)
