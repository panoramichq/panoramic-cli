from collections import namedtuple
from typing import Any, List, Optional

from panoramic.cli.husky.core.tel.visitors.terminal_visitor import TelTerminalVisitor
from panoramic.cli.tel_grammar.TelParser import TelParser as AntlrTelParser

ExceptionDetails = namedtuple('ExceptionDetails', ['position', 'line', 'wrong_symbol', 'expression'])


class TelVisitorHelper:
    @classmethod
    def get_fn_arg_contexts(cls, ctx: AntlrTelParser.FnContext) -> List[Any]:
        """
        Returns only actual argument contexts.
        """
        if len(ctx.children) <= 3:
            # [fn_name,(,)] => 3 children means no args, return empty array
            return []
        else:
            # Skip fnname and '(', step 2 to skip ','
            return ctx.children[2::2]

    @classmethod
    def fn_ctx_number_of_args(cls, ctx: AntlrTelParser.FnContext) -> int:
        """
        Returns number of arguments in fn.
        Example: fn_name(1,2,3) => Return 3
        """
        return len(cls.get_fn_arg_contexts(ctx))

    @classmethod
    def get_exception_details_from_node(cls, node) -> ExceptionDetails:
        position = node.symbol.start + 1
        line = node.symbol.line
        wrong_symbol = str(node.symbol.text)
        expression = str(node.symbol.source[1])
        return ExceptionDetails(position=position, line=line, wrong_symbol=wrong_symbol, expression=expression)

    @classmethod
    def get_exception_details_from_ctx(cls, ctx) -> ExceptionDetails:
        position = ctx.start.start + 1
        line = ctx.start.line
        wrong_symbol = str(ctx.start.text)
        expression = str(ctx.start.source[1])
        return ExceptionDetails(position=position, line=line, wrong_symbol=wrong_symbol, expression=expression)

    @classmethod
    def zeroifnull(cls, expr: Optional[str], apply: bool = True) -> str:
        """
        Wraps the expression into zeroifnull coalesce fn.
        :param expr: original expression
        :param apply: For simpler code on caller, so caller can always call this fn, but control the application by
        this flag.
        """
        if expr and apply:
            return f'coalesce({expr},0)'
        else:
            return expr or ''

    @classmethod
    def render_second_operand(cls, symbol: str, second_operand: str, apply_zero_if_null: bool = False) -> str:
        """
        Helper fn that is just rendering second part of multi and add operation.
        """
        output = f' {symbol} '
        if symbol == '/':
            output += f'NULLIF({second_operand}, 0)'
        else:
            output += cls.zeroifnull(second_operand, apply_zero_if_null)

        return output

    @classmethod
    def use_terminal_visitor(cls, ctx) -> str:
        """
        Runs terminal TEL visitor on the current context and returns its string representation
        """
        v = TelTerminalVisitor()
        v.visitChildren(ctx)

        return v.output_string
