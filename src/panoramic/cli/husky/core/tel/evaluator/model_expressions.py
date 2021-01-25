from typing import Any, Optional

from sqlalchemy import literal_column

from panoramic.cli.husky.core.federated.utils import prefix_with_virtual_data_source
from panoramic.cli.husky.core.sql_alchemy_util import (
    quote_identifier,
    safe_quote_identifier,
)
from panoramic.cli.husky.core.tel.evaluator.ast import TelExpression
from panoramic.cli.husky.core.tel.evaluator.context import TelRootContext
from panoramic.cli.husky.core.tel.evaluator.expressions import TelTaxon
from panoramic.cli.husky.core.tel.evaluator.result import TelQueryResult
from panoramic.cli.husky.core.tel.types.tel_types import TelDataType, TelType


class TelColumn(TelExpression):
    def __init__(self, context: TelRootContext, name: str):
        super().__init__(context)
        self._name = name

    def result(self, context: TelRootContext) -> TelQueryResult:
        column_name = f'{safe_quote_identifier(context.tel_dialect.unique_object_name, context.husky_dialect)}.{quote_identifier(self._name, context.husky_dialect)}'
        return TelQueryResult(literal_column(column_name), context.husky_dialect)

    def return_type(self, context: TelRootContext) -> TelType:
        return TelType(TelDataType.UNKNOWN, False)

    def literal_value(self, context: TelRootContext) -> Optional[Any]:
        return None

    def __repr__(self):
        return f'"{self._name}"'


class TelSQLTaxon(TelTaxon):
    def result(self, context: TelRootContext) -> TelQueryResult:
        sql_expression = literal_column(
            context.tel_dialect.model.taxon_sql_accessor(
                context.husky_context,
                prefix_with_virtual_data_source(context.tel_dialect.virtual_data_source, self._slug),
                False,
                context.tel_dialect,
            )
        )
        return TelQueryResult(sql_expression, context.husky_dialect)

    def invalid_value(self, context: TelRootContext) -> bool:
        return False

    def return_type(self, context: TelRootContext) -> TelType:
        return TelType(TelDataType.UNKNOWN, False)

    def __repr__(self):
        return f'{self._slug}'
