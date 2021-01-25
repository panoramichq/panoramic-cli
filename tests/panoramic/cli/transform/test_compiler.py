from unittest.mock import patch

from panoramic.cli.husky.federated.transform.service import TransformService
from panoramic.cli.transform.compiler import TransformCompiler
from panoramic.cli.transform.pano_transform import PanoTransform


def test_wraps_with_create_view_statement():
    with patch.object(
        TransformService, 'compile_transformation_request', lambda req, c_id, data_source: ('SELECT 1', None)
    ):
        transform = PanoTransform(name='test', fields=['a'], target='connection.schema.view_name')

        transform_compiler = TransformCompiler(company_id="company_id")
        compiled_transform = transform_compiler.compile(transform=transform)

        assert compiled_transform.company_id == 'company_id'
        assert compiled_transform.compiled_query == 'CREATE OR REPLACE VIEW schema.view_name AS (SELECT 1)'
