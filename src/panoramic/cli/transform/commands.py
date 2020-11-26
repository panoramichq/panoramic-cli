from typing import List, Optional

from panoramic.cli.context import get_company_slug
from panoramic.cli.local.reader import FileReader
from panoramic.cli.local.writer import FileWriter
from panoramic.cli.print import echo_error, echo_info
from panoramic.cli.transform.executor import TransformExecutor
from panoramic.cli.transform.pano_transform import PanoTransform


def create_command(name: str, fields: List[str], target: str, filters: Optional[str]):
    transform = PanoTransform(name=name, fields=fields, target=target, filters=filters)

    writer = FileWriter()
    writer.write_transform(transform)


def compile_command():
    company_slug = get_company_slug()
    global_package = FileReader().get_global_package()

    for transform_dict, _transform_path in global_package.read_transforms():
        try:
            transform = PanoTransform.from_dict(transform_dict)
            transform_executor = TransformExecutor.from_transform(transform=transform, company_slug=company_slug)

            # TODO: Add better rendering than just printing the transformed sql out
            echo_info(transform_executor.compiled_query)
        except Exception as e:
            echo_error(str(e))


def exec_command(yes: bool = False):
    company_slug = get_company_slug()
    global_package = FileReader().get_global_package()

    for transform_dict, _transform_path in global_package.read_transforms():
        try:
            transform = PanoTransform.from_dict(transform_dict)
            transform_executor = TransformExecutor.from_transform(transform=transform, company_slug=company_slug)

            echo_info(transform_executor.compiled_query)
            # FIXME: if yes: execute
            if not yes:
                return

            transform_executor.execute()
            # TODO: Add better rendering than just printing the transformed sql out
        except Exception as e:
            echo_error(str(e))
