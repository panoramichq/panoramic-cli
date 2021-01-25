import random
import re
from typing import Dict, Iterable, List, Union

from sqlalchemy import distinct, func
from sqlalchemy.engine import default
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.elements import ColumnElement
from xxhash.cpython import xxh64

from panoramic.cli.husky.core.taxonomy.enums import AggregationType
from panoramic.cli.husky.service.helpers import RUNTIME_DIALECTS
from panoramic.cli.husky.service.types.enums import HuskyQueryRuntime

_DEFAULT_MAX_COLUMN_NAME_LENGTH = 63
"""Max column name length - currently, PG only supports 63 characters per column name"""
_SAFE_IDENTIFIER_HASH_SIZE = 18
"""Length of unique hash appended to safe identifiers"""


def _random_bind_param_prefix():
    return str(random.randint(1, 1000000)) + '_'


def get_unique_bind_param_name(param_name: str):
    """
    Generating random prefix for filters, to ensure they are globally unique in any composed query.
    https://stackoverflow.com/questions/58203712/sqlalchemy-conflicting-bind-param-names
    """
    return f'{_random_bind_param_prefix()}{param_name}'


def sort_columns(selectors: List[Union[str, ColumnElement]]) -> List[Union[str, ColumnElement]]:
    """
    Alphabetically sorts the selectors, so writing tests is a bit easier.
    """

    def get_selector_label(selector: Union[str, ColumnElement]) -> str:
        if isinstance(selector, ColumnElement):
            return selector.key or str(selector)
        elif isinstance(selector, str):
            return selector
        else:
            raise RuntimeError('Unexpected selector ' + str(selector))

    return sorted(selectors, key=get_selector_label)


def compile_query(
    clause: ClauseElement,
    dialect: default.DefaultDialect = RUNTIME_DIALECTS[HuskyQueryRuntime.snowflake],
    literal_binds: bool = True,
) -> str:
    """
    Compile the query and bind all parameters to it.

    !!! WARNING !!!
    Do not execute the returned query
    """
    if clause is not None and isinstance(clause, ClauseElement):
        return str(clause.compile(compile_kwargs={"literal_binds": literal_binds}, dialect=dialect))
    else:
        return clause


def quote_identifier(value, dialect: default.DefaultDialect):
    """Conditionally quote an identifier.

    The identifier is quoted if it is a reserved word, contains
    quote-necessary characters, or is an instance of
    :class:`.quoted_name` which includes ``quote`` set to ``True``.
    """
    return dialect.identifier_preparer.quote(value)


def quote_fully_qualified_name(value: str, dialect: default.DefaultDialect, separator: str = '.'):
    """
    Splits fully qualified name into parts, and calls `quote_identifier` on each,
    and then joins them back together
    """

    return separator.join(map(lambda part: quote_identifier(part, dialect=dialect), value.split(separator)))


UNSAFE_IDENTIFIER_CHARS_REGEXP = re.compile('[^0-9a-zA-Z_]')
"""
Identifier must contain only letters, numbers and underscores.
"""


def safe_identifier(val):
    """
    Returns safe identifier name across DBs (using only characters 0-9a-zA-Z & length 128 chars).
    """
    if len(val) == 0:
        return val
    if not val[0].isalpha() and val[0] != '_':
        val = "_" + val

    val_safe = UNSAFE_IDENTIFIER_CHARS_REGEXP.sub('_', val).lower()

    if val_safe != val:
        # If slugifying changes the value, it means we call it for the first time, and to avoid
        # possible collisions, we need to add hash of the original value,
        # so then spend+1 and spend-1 have different hash and thus different identifier.
        # NOTE: make sure there's room left for the unique hash at the end of the column name
        # (unless we're in TEL debug model)
        value_before_hash = val_safe[: _DEFAULT_MAX_COLUMN_NAME_LENGTH - _SAFE_IDENTIFIER_HASH_SIZE - 1]
        # calculate the hash
        hash_from_val = xxh64(val).hexdigest()[:_SAFE_IDENTIFIER_HASH_SIZE]
        return f'{value_before_hash}_{hash_from_val}'
    else:
        # In case it did not change, we return the original and dont add hash. That way,
        # safe_identifier_name(safe_identifier_name(x)) == safe_identifier_name(x), which is a huge benefit.
        return val


def safe_identifiers_iterable(val_list: Iterable[str]) -> List[str]:
    """
    Returns new list, all with safe identifiers.
    """
    return [safe_identifier(val) for val in val_list]


def safe_quote_identifier(value, dialect: default.DefaultDialect):
    """
    For anything at the moment, only adds quotes if necessary.
    Do not use as argument to sqlalchemy's column()
    """
    value = safe_identifier(value)
    return dialect.identifier_preparer.quote(value)


AGGREGATION_TYPE_TO_SQLALCHEMY_FN: Dict[AggregationType, func.Function] = {
    AggregationType.sum: func.SUM,
    AggregationType.avg: func.AVG,
    AggregationType.min: func.MIN,
    AggregationType.max: func.MAX,
    AggregationType.count_all: func.count,
    AggregationType.count_distinct: lambda col: func.count(distinct(col)),
}

LIKE_PATTERN_ESCAPE_CHAR = '/'
"""
Special characters in LIKE operator can be escaped with this character.
"""

_ESCAPE_RULES_TRANSLATION_TABLE = str.maketrans(
    {'%': LIKE_PATTERN_ESCAPE_CHAR + '%', '_': LIKE_PATTERN_ESCAPE_CHAR + '_'}
)


def escape_special_character_in_like_pattern(pattern: str):
    return pattern.translate(_ESCAPE_RULES_TRANSLATION_TABLE)
