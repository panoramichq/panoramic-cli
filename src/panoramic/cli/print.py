import traceback
from enum import Enum
from typing import Any, List, Optional

import click


class Color(Enum):

    RED = 'red'
    GREEN = 'green'
    YELLOW = 'yellow'
    BLUE = 'blue'


def echo_warning(msg: str):
    click.echo(f'Warning: {msg}')


def echo_error(msg: str, exc_info=False):
    click.echo(f'Error: {msg}')

    if exc_info:
        click.echo(traceback.format_exc())


def echo_info(msg: str):
    click.echo(msg)


def echo_style(msg: str, fg: Optional[Color] = None, nl: bool = True):
    """
    Print colored output.

    Should not be active while progress bar is visible.
    """
    fg_value = fg.value if fg is not None else None
    click.secho(msg, fg=fg_value, nl=nl)


def echo_warnings(errors: List[Any]):
    for error in errors:
        echo_info('')
        echo_warning(str(error))


def echo_errors(errors: List[Any]):
    for error in errors:
        echo_info('')  # visually separate errors
        echo_error(str(error))
