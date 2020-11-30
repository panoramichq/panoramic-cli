from pathlib import Path
from typing import List, Optional, Tuple

import click
from tqdm import tqdm

from panoramic.cli.context import get_company_slug
from panoramic.cli.local.reader import FileReader
from panoramic.cli.local.writer import FileWriter
from panoramic.cli.print import echo_info
from panoramic.cli.transform.executor import TransformExecutor
from panoramic.cli.transform.pano_transform import PanoTransform


def create_command(name: str, fields: List[str], target: str, filters: Optional[str]):
    transform = PanoTransform(name=name, fields=fields, target=target, filters=filters)

    writer = FileWriter()
    writer.write_transform(transform)


def exec_command(
    compile_only: bool = False,
    yes: bool = False,
):
    company_slug = get_company_slug()
    global_package = FileReader().get_global_package()
    executors: List[Tuple[TransformExecutor, Path]] = []

    parsed_transforms = [
        (PanoTransform.from_dict(transform_dict), transform_path)
        for transform_dict, transform_path in global_package.read_transforms()
    ]

    sorted_transforms = sorted(parsed_transforms, key=lambda pair: pair[0].connection_name)

    echo_info('Compiling transforms...')
    with tqdm(sorted_transforms) as compiling_bar:
        for transform, transform_path in compiling_bar:
            try:
                transform_executor = TransformExecutor.from_transform(transform=transform, company_slug=company_slug)

                compiling_bar.write(f'[{transform.connection_name}] {transform_executor.compiled_query}')
                compiling_bar.write('')
                executors.append((transform_executor, transform_path))
            except Exception as e:
                compiling_bar.write(f'\nError: Failed to compile transform {transform_path}:\n  {str(e)}')

    if compile_only or not yes and not click.confirm('Do you want to execute transforms?'):
        return

    echo_info('Executing transforms...')
    with tqdm(executors) as exec_bar:
        for (transform_executor, transform_path) in exec_bar:
            try:
                exec_bar.write(
                    f'Executing: {transform_executor.transform.name} on {transform_executor.transform.connection_name}'
                )
                transform_executor.execute()
                exec_bar.write(f'\u2713 [{transform_executor.transform.connection_name}] {transform.name}')
            except Exception as e:
                exec_bar.write(f'\nError: Failed to execute transform {transform_path}:\n  {str(e)}')
