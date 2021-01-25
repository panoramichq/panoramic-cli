from sqlalchemy import Date, String, literal, literal_column, not_
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import ColumnElement
from sqlalchemy.sql.compiler import SQLCompiler
from sqlalchemy.sql.elements import BindParameter, TextClause
from sqlalchemy.sql.expression import FunctionElement, cast

from panoramic.cli.husky.core.sql_alchemy_util import get_unique_bind_param_name
from panoramic.cli.husky.service.types.enums import HuskyQueryRuntime


class Parentheses(ColumnElement):
    def __init__(self, element):
        self.element = element

    def _negate(self):
        return Parentheses(not_(self.element))

    def operate(self, op, *other, **kwargs):
        return Parentheses(self.element.operate(op, *other, **kwargs))


@compiles(Parentheses)
def compile_parentheses(element: Parentheses, compiler: SQLCompiler, **kw):
    return "(%s)" % compiler.process(element.element, **kw)


class MyTextClause(TextClause):
    """
    Improved TextClause class. It generates unique binding names for all the bindings.
    Fix for https://stackoverflow.com/questions/58203712/sqlalchemy-conflicting-bind-param-names
    """

    def __init__(self, text, bind=None):
        self._bind = bind
        self._bindparams = {}
        self._original_to_unique_param_map = {}

        def repl(m):
            original = m.group(1)
            unique = get_unique_bind_param_name(m.group(1))
            self._original_to_unique_param_map[original] = unique
            self._bindparams[m.group(1)] = BindParameter(unique)
            return ":%s" % unique

        # scan the string and search for bind parameter names, add them
        # to the list of bindparams
        text_unique_params = self._bind_params_regex.sub(repl, text)
        super().__init__(text_unique_params, bind)

    def bindparams(self, *binds, **orig_names_to_values):
        unique_names_to_values = {
            self._original_to_unique_param_map[key]: value for key, value in orig_names_to_values.items()
        }
        return super().bindparams(*binds, **unique_names_to_values)


my_sql_text = MyTextClause


class NullIf(FunctionElement):
    name = 'nullif'


@compiles(NullIf, HuskyQueryRuntime.snowflake.value)
@compiles(NullIf, HuskyQueryRuntime.bigquery.value)
def compile_nullif(element: NullIf, compiler: SQLCompiler, **kw):
    return "nullif(%s)" % compiler.process(element.clauses, **kw)


class ConvertTimezone(FunctionElement):
    name = 'convert_timezone'

    def __init__(self, expr, tz_from, tz_to):
        self.expr = expr
        self.tz_from = tz_from
        self.tz_to = tz_to


@compiles(ConvertTimezone, HuskyQueryRuntime.snowflake.value)
def compile_convert_timezone_snowflake(element: ConvertTimezone, compiler: SQLCompiler, **kw):
    if element.tz_to is not None:
        return "convert_timezone(%s, %s, %s)" % (
            compiler.process(element.tz_from, **kw),
            compiler.process(element.tz_to, **kw),
            compiler.process(element.expr, **kw),
        )
    else:
        return "convert_timezone(%s, %s)" % (
            compiler.process(element.tz_from, **kw),
            compiler.process(element.expr, **kw),
        )


@compiles(ConvertTimezone, HuskyQueryRuntime.bigquery.value)
def compile_convert_timezone_bigquery(element: ConvertTimezone, compiler: SQLCompiler, **kw):
    if element.tz_to is not None:
        return "datetime(datetime(%s, %s), %s)" % (
            compiler.process(element.expr, **kw),
            compiler.process(element.tz_from, **kw),
            compiler.process(element.tz_to, **kw),
        )
    else:
        return "datetime(%s, %s)" % (compiler.process(element.expr, **kw), compiler.process(element.tz_from, **kw))


class SplitPart(FunctionElement):
    name = 'split_part'

    def __init__(self, expr, delimiter, position):
        self.expr = expr
        self.delimiter = delimiter
        self.position = position


@compiles(SplitPart, HuskyQueryRuntime.snowflake.value)
def compile_split_part_snowflake(element: SplitPart, compiler: SQLCompiler, **kw):
    return "split_part(%s, %s, %s)" % (
        compiler.process(element.expr, **kw),
        compiler.process(element.delimiter, **kw),
        compiler.process(element.position, **kw),
    )


@compiles(SplitPart, HuskyQueryRuntime.bigquery.value)
def compile_split_part_bigquery(element: SplitPart, compiler: SQLCompiler, **kw):
    return "split(%s, %s)[safe_ordinal(%s)]" % (
        compiler.process(element.expr, **kw),
        compiler.process(element.delimiter, **kw),
        compiler.process(element.position, **kw),
    )


class DateTrunc(FunctionElement):
    name = 'date_trunc'

    def __init__(self, part, expr):
        self.part = part
        self.expr = expr

    @property
    def bigquery_part(self):
        if self.part == 'weekday':
            return 'weekday(MONDAY)'
        else:
            return self._part


@compiles(DateTrunc, HuskyQueryRuntime.snowflake.value)
def compile_date_trunk_snowflake(element: DateTrunc, compiler: SQLCompiler, **kw):
    return "date_trunc(%s, %s)" % (compiler.process(literal(element.part), **kw), compiler.process(element.expr, **kw))


@compiles(DateTrunc, HuskyQueryRuntime.bigquery.value)
def compile_date_trunk_bigquery(element: DateTrunc, compiler: SQLCompiler, **kw):
    return "datetime_trunc(%s, %s)" % (
        compiler.process(element.expr, **kw),
        compiler.process(literal_column(element.bigquery_part), **kw),
    )


class ParseDate(FunctionElement):
    name = 'parse_date'

    def __init__(self, expr, format):
        self.expr = expr
        self.format = format


@compiles(ParseDate, HuskyQueryRuntime.snowflake.value)
def compile_parse_date_snowflake(element: ParseDate, compiler: SQLCompiler, **kw):
    if element.format is not None:
        return "to_date(%s, %s)" % (compiler.process(element.expr, **kw), compiler.process(element.format, **kw))
    else:
        return "to_date(%s)" % compiler.process(element.expr, **kw)


@compiles(ParseDate, HuskyQueryRuntime.bigquery.value)
def compile_parse_date_bigquery(element: ParseDate, compiler: SQLCompiler, **kw):
    if element.format is not None:
        return "parse_date(%s, %s)" % (compiler.process(element.format, **kw), compiler.process(element.expr, **kw))
    else:
        return compiler.process(cast(element.expr, Date()), **kw)


class TimestampDiff(FunctionElement):
    name = 'timestamp_diff'

    def __init__(self, time_unit, start, end):
        self.part = time_unit
        self.start = start
        self.end = end


@compiles(TimestampDiff, HuskyQueryRuntime.snowflake.value)
def compile_timestamp_diff_snowflake(element: TimestampDiff, compiler: SQLCompiler, **kw):
    return "timestampdiff(%s, %s, %s)" % (
        compiler.process(literal_column(element.part, String()), **kw),
        compiler.process(element.start, **kw),
        compiler.process(element.end, **kw),
    )


@compiles(TimestampDiff, HuskyQueryRuntime.bigquery.value)
def compile_timestamp_diff_bigquery(element: TimestampDiff, compiler: SQLCompiler, **kw):
    return "timestampdiff(%s, %s, %s)" % (
        compiler.process(element.start, **kw),
        compiler.process(element.end, **kw),
        compiler.process(literal(element.part, String()), **kw),
    )


class Extract(FunctionElement):
    name = 'date_part'

    def __init__(self, part, expr):
        self.part = part
        self.expr = expr


@compiles(Extract, HuskyQueryRuntime.snowflake.value)
def compile_extract_snowflake(element: Extract, compiler: SQLCompiler, **kw):
    return "date_part('%s', %s)" % (
        compiler.process(literal_column(element.part), **kw),
        compiler.process(element.expr, **kw),
    )


@compiles(Extract, HuskyQueryRuntime.bigquery.value)
def compile_extract_bigquery(element: Extract, compiler: SQLCompiler, **kw):
    return "extract(%s FROM %s)" % (
        compiler.process(literal_column('DAYOFWEEK' if element.part == 'DOW' else element.part), **kw),
        compiler.process(element.expr, **kw),
    )
