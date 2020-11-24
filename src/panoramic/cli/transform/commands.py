from typing import List, Optional

from panoramic.cli.context import get_company_slug
from panoramic.cli.local.reader import FileReader
from panoramic.cli.local.writer import FileWriter
from panoramic.cli.print import echo_info
from panoramic.cli.transform.pano_transform import PanoTransform
from panoramic.cli.transform_executor import TransformExecutor


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
            transform_query = TransformExecutor.compile(
                transform=transform,
                company_slug=company_slug,
            )
            # TODO: Add better rendering than just printing the transformed sql out
            echo_info(transform_query)
        except Exception:
            pass


def exec_command():
    company_slug = get_company_slug()
    global_package = FileReader().get_global_package()

    for transform_dict, _transform_path in global_package.read_transforms():
        try:
            transform = PanoTransform.from_dict(transform_dict)
            transform_query = TransformExecutor.compile(
                transform=transform,
                company_slug=company_slug,
            )
            echo_info(transform_query)
            # FIXME: if yes: execute
            TransformExecutor.execute(transform=transform, query=transform_query)
            # TODO: Add better rendering than just printing the transformed sql out
        except Exception:
            pass
