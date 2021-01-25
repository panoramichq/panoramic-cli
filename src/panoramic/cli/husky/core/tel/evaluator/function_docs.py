from collections import OrderedDict
from typing import Iterable, cast

from docstring_parser import parse

from panoramic.cli.husky.core.tel.evaluator.function_specs import (
    AcceptedArg,
    TelTypedFunction,
)
from panoramic.cli.husky.core.tel.evaluator.functions import TEL_FUNCTIONS


def function_definitions():
    result = {}
    for fun_name, fun in OrderedDict(sorted(TEL_FUNCTIONS.items())).items():
        docstring = parse(fun.__doc__)
        expected_args = []
        phase = None
        return_type = None
        invalid_value = None

        if issubclass(fun, TelTypedFunction):
            tfun = cast(TelTypedFunction, fun)
            expected_args = tfun.expected_arg_types
            phase = tfun.phase_spec
            return_type = tfun.return_type_spec
            invalid_value = tfun.invalid_value_spec

        description = {
            'arguments': [
                {
                    'name': param.arg_name,
                    'typeName': param.type_name,
                    'description': param.description,
                    **(_tel_arg_remote_spec(param.arg_name, expected_args) or {}),
                }
                for param in docstring.params
            ],
            'raises': [{'typeName': exc.type_name, 'description': exc.description} for exc in docstring.raises],
            'shortDescription': docstring.short_description,
            'longDescription': docstring.long_description,
            'returns': {
                'typeName': docstring.returns.type_name,
                'description': docstring.returns.description,
                **(return_type.to_remote_spec() if return_type else {}),
            },
            **({'phase': phase.to_remote_spec()} if phase else {}),
            **({'invalidValue': invalid_value.to_remote_spec()} if invalid_value else {}),
        }

        result[fun_name] = description

    return result


def _tel_arg_remote_spec(arg_name: str, expected_args: Iterable[AcceptedArg]):
    for arg in expected_args:
        if arg.name == arg_name:
            return arg.to_remote_spec()
