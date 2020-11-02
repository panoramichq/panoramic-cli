import difflib

from panoramic.cli.file_utils import dump_yaml
from panoramic.cli.print import Color, echo_info, echo_style
from panoramic.cli.state import ActionList

_LINE_START_TO_COLOR = {
    '+': Color.GREEN,
    '-': Color.RED,
    '@': Color.BLUE,
}


def echo_diff(actions: ActionList):
    for action in actions.actions:
        if action.is_deletion:
            echo_style(action.description, fg=Color.RED)
        elif action.is_creation:
            echo_style(action.description, fg=Color.GREEN)
        else:
            # Assumes update
            echo_style(action.description, fg=Color.YELLOW)

            current_yaml = dump_yaml(action.current.to_dict()) if action.current is not None else ''
            desired_yaml = dump_yaml(action.desired.to_dict()) if action.desired is not None else ''

            assert current_yaml is not None and desired_yaml is not None

            current_yaml_lines = current_yaml.splitlines(keepends=True)
            desired_yaml_lines = desired_yaml.splitlines(keepends=True)

            diff = difflib.unified_diff(current_yaml_lines, desired_yaml_lines, fromfile='current', tofile='desired')

            for line in diff:
                color = _LINE_START_TO_COLOR.get(line[0])
                echo_style(line, fg=color, nl=False)

        echo_info('')
