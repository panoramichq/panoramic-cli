from pathlib import Path
from typing import List, Tuple

import click
from tqdm import tqdm

from panoramic.cli.connections import Connections
from panoramic.cli.context import get_company_slug
from panoramic.cli.local.reader import FileReader
from panoramic.cli.local.writer import FileWriter
from panoramic.cli.paths import FileExtension, Paths
from panoramic.cli.print import echo_error, echo_info
from panoramic.cli.transform.executor import TransformExecutor
from panoramic.cli.transform.pano_transform import PanoTransform


def create_command():
    echo_info('Scaffolding a new transform...')
    name = click.prompt('name: ')

    connections = Connections.load()
    connection_names = connections.keys() if connections else []
    connection_base_text = 'connection: '

    if len(connection_names) == 0:
        connection_prompt_text = connection_base_text
    elif len(connection_names) > 3:
        connection_prompt_text = f'{connection_base_text} (Available - {{{",".join(list(connection_names)[:3])}}},...)'
    else:
        connection_prompt_text = f'{connection_base_text} (Available - {{{",".join(connection_names)}}})'

    # Assemble target based on input
    connection = click.prompt(connection_prompt_text)

    schema = click.prompt('schema:', default='pano')  # Fixme: think through a default schema?
    view = click.prompt('view: ')
    target = f'{connection}.{schema}.{view}'

    transform = PanoTransform(name=name, fields=[], target=target)
    writer = FileWriter()
    transform_path = Paths.transforms_dir() / f'{transform.name}{FileExtension.TRANSFORM_YAML.value}'

    if not Path.exists(transform_path):
        writer.write_transform(transform)
    else:
        echo_error(f'Transform {transform_path} already exists')


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
                exec_bar.write(
                    f'\u2717 [{transform_executor.transform.connection_name}] {transform.name} \nError: Failed to execute transform {transform_path}:\n  {str(e)}'
                )
