from pathlib import Path
from typing import List, Tuple

import click
from tqdm import tqdm

from panoramic.cli.config.companies import get_company_id
from panoramic.cli.connections import Connections
from panoramic.cli.local.get import get_transforms
from panoramic.cli.local.writer import FileWriter
from panoramic.cli.paths import FileExtension, Paths
from panoramic.cli.print import echo_error, echo_info
from panoramic.cli.transform.compiler import TransformCompiler
from panoramic.cli.transform.executor import TransformExecutor
from panoramic.cli.transform.pano_transform import CompiledTransform, PanoTransform


def create_command():
    echo_info('Scaffolding a new transform...')
    name = click.prompt('name')

    connections = Connections.load()
    connection_names = connections.keys() if connections else []
    connection_base_text = 'connection'

    if len(connection_names) == 0:
        connection_prompt_text = connection_base_text
    elif len(connection_names) > 3:
        connection_prompt_text = f'{connection_base_text} (Available - {{{",".join(list(connection_names)[:3])}}},...)'
    else:
        connection_prompt_text = f'{connection_base_text} (Available - {{{",".join(connection_names)}}})'

    # Assemble target based on input
    connection = click.prompt(connection_prompt_text)

    target_view_path = click.prompt(f'target: {connection}.', prompt_suffix="")
    target = f'{connection}.{target_view_path}'

    transform = PanoTransform(name=name, fields=[], target=target)
    writer = FileWriter()
    transform_path = Paths.transforms_dir() / f'{transform.name}{FileExtension.TRANSFORM_YAML.value}'

    if Path.exists(transform_path):
        echo_error(f'Transform {transform_path} already exists')
    else:
        writer.write_transform(transform)


def exec_command(
    compile_only: bool = False,
    yes: bool = False,
):
    compiled_transforms: List[Tuple[CompiledTransform, Path]] = []

    transforms_with_path = get_transforms()

    if len(transforms_with_path) == 0:
        echo_info('No transforms found...')
        return

    transform_compiler = TransformCompiler(get_company_id())

    file_writer = FileWriter()

    echo_info('Compiling transforms...')
    with tqdm(transforms_with_path) as compiling_bar:
        for transform, transform_path in compiling_bar:
            try:
                compiled_transform = transform_compiler.compile(transform=transform)
                compiled_transforms.append((compiled_transform, transform_path))

                compiled_sql_path = file_writer.write_compiled_transform(compiled_transform)

                compiling_bar.write(f'[{transform.name}] writing compiled query to {compiled_sql_path}')
            except Exception as e:
                compiling_bar.write(f'\nError: Failed to compile transform {transform_path}:\n  {str(e)}')

    if len(compiled_transforms) == 0:
        echo_info('No transforms to execute...')
        return

    if compile_only or not yes and not click.confirm('Do you want to execute transforms?'):
        return

    echo_info('Executing transforms...')
    with tqdm(compiled_transforms) as exec_bar:
        for (compiled_transform, transform_path) in exec_bar:
            try:
                exec_bar.write(
                    f'Executing: {compiled_transform.transform.name} on {compiled_transform.transform.connection_name}'
                )

                TransformExecutor.execute(compiled_transform)
                exec_bar.write(f'\u2713 [{compiled_transform.transform.connection_name}] {transform.name}')
            except Exception as e:
                exec_bar.write(
                    f'\u2717 [{compiled_transform.transform.connection_name}] {transform.name} \nError: Failed to execute transform {transform_path}:\n  {str(e)}'
                )
