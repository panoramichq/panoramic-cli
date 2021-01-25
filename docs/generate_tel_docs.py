from collections import OrderedDict

from docstring_parser import parse

from panoramic.cli.husky.core.tel.evaluator.functions import TEL_FUNCTIONS

with open('docs/tel-functions.md', 'w') as f:
    print('# Supported TEL Functions', file=f)

    for fun_name, fun in OrderedDict(sorted(TEL_FUNCTIONS.items())).items():
        docstring = parse(fun.__doc__)

        arguments = '\n'.join(
            [f'`{param.arg_name}` | {param.type_name} | {param.description}' for param in docstring.params]
        )
        raises = '\n'.join([f'{exc.description} |' for exc in docstring.raises])

        print(
            f'''
### `{fun_name}`: {docstring.short_description}

Supported dialects: {', '.join(sorted(fun._supported_dialects))}

{docstring.long_description}

**Arguments**

Name | Type | Description
---- | ---- | ------------
{arguments}

**Returns**:
(*{docstring.returns.type_name if docstring.returns else None}* type) {docstring.returns.description if docstring.returns else None}.

**Raises Validation Error**

When |
---- |
{raises}
        ''',
            file=f,
        )
