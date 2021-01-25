from abc import abstractmethod
from operator import eq, ge, gt, le, lt, ne
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Type, Union

from schematics import Model
from schematics.types import (
    BooleanType,
    FloatType,
    IntType,
    ListType,
    PolyModelType,
    StringType,
)
from sqlalchemy import String, and_
from sqlalchemy import cast as sql_cast
from sqlalchemy import func, literal, literal_column, not_, or_, text
from sqlalchemy.sql import ClauseElement, Select

from panoramic.cli.husky.common.enum import EnumHelper
from panoramic.cli.husky.common.exception_enums import (
    ExceptionErrorCode,
    ExceptionGroup,
    ExceptionSeverity,
)
from panoramic.cli.husky.core.schematics.model import EnumType, UnionNoConversionType
from panoramic.cli.husky.core.sql_alchemy_util import LIKE_PATTERN_ESCAPE_CHAR
from panoramic.cli.husky.service.filter_builder.enums import (
    ArrayFilterOperator,
    FilterClauseType,
    LogicalOperator,
    SimpleFilterOperator,
)
from panoramic.cli.husky.service.select_builder.taxon_model_info import TaxonModelInfo
from panoramic.cli.husky.service.utils.exceptions import HuskyException

SIMPLE_OPERATORS_FUNCTIONS: Dict[SimpleFilterOperator, Callable] = {
    SimpleFilterOperator.EQ: eq,
    SimpleFilterOperator.NEQ: ne,
    SimpleFilterOperator.GT: gt,
    SimpleFilterOperator.LT: lt,
    SimpleFilterOperator.GTE: ge,
    SimpleFilterOperator.LTE: le,
}

GROUP_OPERATORS_FUNCTIONS: Dict[LogicalOperator, Callable] = {LogicalOperator.AND: and_, LogicalOperator.OR: or_}


def _generate_simple_operator_clause(
    ctx, clause: 'FilterClause', taxon_model_info_map: Dict[str, TaxonModelInfo]
) -> ClauseElement:
    """
    :param ctx: Husky query context
    :param clause: Filter clause
    :param taxon_model_info_map: map of taxons and model infos
    :return: SQL clause element
    """
    taxon_model_info: TaxonModelInfo = taxon_model_info_map[clause.taxon]

    if isinstance(clause, TaxonTaxonFilterClause):
        # Always referring to the full column name, never relying on alchemy.column reference to not be ambiguous.
        right_taxon_info = taxon_model_info_map[clause.right_taxon]
        right_operand = literal_column(right_taxon_info.taxon_sql_accessor)
    else:
        right_operand = literal(clause.value) if clause.value is not None else None

    # Always referring to the full column name, never relying on alchemy.column reference to not be ambiguous.
    left_operand = literal_column(taxon_model_info.taxon_sql_accessor)

    if clause.operator == SimpleFilterOperator.EQ and taxon_model_info.is_array:
        # Otherwise in SF, if taxon is an array, instead of using equal sign(=),
        # we check if the value is obtained in the taxon value (list)
        # First argument to ARRAY_CONTAINS needs to be of type VARIANT
        return func.array_contains(right_operand.op('::')(text('VARIANT')), left_operand)
    if clause.operator in SIMPLE_OPERATORS_FUNCTIONS:
        if right_operand is None:  # support "IS NULL" and " IS NOT NULL"
            if clause.operator is SimpleFilterOperator.EQ:
                return left_operand.is_(None)
            elif clause.operator is SimpleFilterOperator.NEQ:
                return not_(left_operand.is_(None))
            else:
                raise UnknownOperator(clause)

        return SIMPLE_OPERATORS_FUNCTIONS[clause.operator](left_operand, right_operand)
    else:
        # when using LIKE/NOT LIKE operators, we need right operand
        if right_operand is None:
            raise UnknownOperator(clause)

        # When using *like, always cast operand to string.
        left_operand = sql_cast(left_operand, String)

        # LIKE operator is handled differently
        # We should not call escape_special_character_in_like_pattern, coz Fe already does that.
        if clause.operator is SimpleFilterOperator.LIKE:
            return left_operand.like(right_operand, escape=LIKE_PATTERN_ESCAPE_CHAR)
        elif clause.operator is SimpleFilterOperator.NOT_LIKE:
            return not_(left_operand.like(right_operand, escape=LIKE_PATTERN_ESCAPE_CHAR))
        elif clause.operator is SimpleFilterOperator.ILIKE:
            return left_operand.ilike(right_operand, escape=LIKE_PATTERN_ESCAPE_CHAR)
        elif clause.operator is SimpleFilterOperator.NOT_ILIKE:
            return not_(left_operand.ilike(right_operand, escape=LIKE_PATTERN_ESCAPE_CHAR))

    raise UnknownOperator(clause)


class FilterClause(Model):
    """Filter clause representing simple or a nested filter clause

    NOTE: Since class Model is already abstract class, this class does not need to specify it.
    """

    type: FilterClauseType = EnumType(FilterClauseType, required=True)
    """ Type of the filter clause """

    @abstractmethod
    def generate(self, ctx, query: Select, taxon_model_info_map: Dict[str, TaxonModelInfo]) -> ClauseElement:
        """ Generates SQL Alchemy representation of this filter clause"""
        raise NotImplementedError('Not implemented')

    @abstractmethod
    def get_taxon_slugs(self) -> Set[str]:
        """
        Gets all taxons used in this filter clause (and all filter clauses nested within it)
        """
        raise NotImplementedError('Not implemented')

    @staticmethod
    @abstractmethod
    def _claim_polymorphic(data: Dict[str, Any]) -> Optional[Type['FilterClause']]:
        """
        This method is used by PolyModelType to determine which model should be used to represent
        the structure during conversion.

        We could use .claim_polymorphic() instead, but then this method needs to know about all possible models.
        This is not an ideal so we opted for overriding this protected method.

        :param data: Data to be converted
        :return: Model class representing the data
        """
        raise NotImplementedError('Not implemented')

    @staticmethod
    def _detect_filter_clause_type(
        data: Dict[str, Any], expected_type: FilterClauseType, filter_clause: Type['FilterClause']
    ) -> Optional[Type['FilterClause']]:
        if 'type' not in data:
            return None

        if EnumHelper.from_value_safe(FilterClauseType, data['type']) == expected_type:
            return filter_clause

        return None


class TaxonValueFilterClause(FilterClause):
    """Filter clause which represents simple clause with one column and one value connected by simple operator """

    taxon: str = StringType(required=True, min_length=1)
    """Column name in the clause"""

    operator: SimpleFilterOperator = EnumType(SimpleFilterOperator, required=True)
    """Operator in the clause"""

    value: Optional[Union[str, float, bool]] = UnionNoConversionType((IntType, FloatType, StringType, BooleanType))
    """Comparison value in the clause"""

    def generate(self, ctx, query, taxon_model_info_map: Dict[str, TaxonModelInfo]) -> ClauseElement:
        return _generate_simple_operator_clause(ctx, self, taxon_model_info_map)

    def get_taxon_slugs(self) -> Set[str]:
        return {self.taxon}

    @staticmethod
    def _claim_polymorphic(data: Dict[str, Any]) -> Optional[Type['FilterClause']]:
        return FilterClause._detect_filter_clause_type(data, FilterClauseType.TAXON_VALUE, TaxonValueFilterClause)


class TaxonTaxonFilterClause(FilterClause):
    """Filter clause which represents clause including two columns connected by a simple operator"""

    taxon: str = StringType(required=True, min_length=1)
    """Left-side column name"""

    right_taxon: str = StringType(required=True, min_length=1)
    """Right-side column name"""

    operator: SimpleFilterOperator = EnumType(SimpleFilterOperator, required=True)
    """Operator in the clause"""

    def generate(self, ctx, query, taxon_model_info_map: Dict[str, TaxonModelInfo]) -> ClauseElement:
        return _generate_simple_operator_clause(ctx, self, taxon_model_info_map)

    def get_taxon_slugs(self) -> Set[str]:
        return {self.taxon, self.right_taxon}

    @staticmethod
    def _claim_polymorphic(data: Dict[str, Any]) -> Optional[Type['FilterClause']]:
        return FilterClause._detect_filter_clause_type(data, FilterClauseType.TAXON_TAXON, TaxonTaxonFilterClause)


class TaxonArrayFilterClause(FilterClause):
    """Filter clause which represents an array-like clause in SQL (like IN)"""

    taxon: str = StringType(required=True)
    """Column name in the clause"""

    operator: ArrayFilterOperator = EnumType(ArrayFilterOperator, required=True)
    """Specifies operator in the clause"""

    value: Iterable[Union[str, float]] = ListType(StringType(required=True, min_length=1), min_size=1)
    """Value in the array clause"""

    def generate(self, ctx, query, taxon_model_info_map: Dict[str, TaxonModelInfo]) -> ClauseElement:
        taxon_model_info = taxon_model_info_map[self.taxon]

        left_operand = literal_column(taxon_model_info.taxon_sql_accessor)

        if taxon_model_info.is_array:
            # If taxon is an array, instead of using IN operator
            # we check if any of the value is obtained in the taxon value (list)
            if self.operator is ArrayFilterOperator.IN:
                return func.arrays_overlap(func.array_construct(self.value), left_operand)
            if self.operator is ArrayFilterOperator.NOT_IN:
                return not_(func.arrays_overlap(func.array_construct(self.value), left_operand))
        else:
            if self.operator is ArrayFilterOperator.IN:
                return left_operand.in_(self.sql_value)
            if self.operator is ArrayFilterOperator.NOT_IN:
                return not_(left_operand.in_(self.sql_value))

        raise UnknownOperator(self)

    def get_taxon_slugs(self) -> Set[str]:
        return {self.taxon}

    @property
    def sql_value(self):
        return [literal(v) for v in self.value]

    @staticmethod
    def _claim_polymorphic(data: Dict[str, Any]) -> Optional[Type['FilterClause']]:
        return FilterClause._detect_filter_clause_type(data, FilterClauseType.TAXON_ARRAY, TaxonArrayFilterClause)


class GroupFilterClause(FilterClause):
    """Filter clause which represents a group of filter clauses connected by specified operator"""

    logical_operator: LogicalOperator = EnumType(LogicalOperator, required=True)
    """ Logical operator connecting clauses"""

    clauses: List[FilterClause] = ListType(
        PolyModelType(['GroupFilterClause', TaxonValueFilterClause, TaxonTaxonFilterClause, TaxonArrayFilterClause]),
        required=True,
        min_size=1,
    )
    """ List of clauses """

    negate: bool = BooleanType(default=False)
    """ Negate the whole group of clauses """

    def generate(self, ctx, query: Select, taxon_model_info_map: Dict[str, TaxonModelInfo]) -> ClauseElement:
        if self.logical_operator not in GROUP_OPERATORS_FUNCTIONS:
            raise UnknownOperator(self)

        clause = GROUP_OPERATORS_FUNCTIONS[self.logical_operator](
            clause.generate(ctx, query, taxon_model_info_map) for clause in self.clauses
        ).self_group()

        if self.negate:
            return not_(clause)

        return clause

    def get_taxon_slugs(self) -> Set[str]:
        return set(taxon_slug for clause in self.clauses for taxon_slug in clause.get_taxon_slugs())

    @staticmethod
    def _claim_polymorphic(data: Dict[str, Any]) -> Optional[Type['FilterClause']]:
        return FilterClause._detect_filter_clause_type(data, FilterClauseType.GROUP, GroupFilterClause)


AllOperatorsType = Union[SimpleFilterOperator, ArrayFilterOperator, LogicalOperator]


class UnknownOperator(HuskyException):
    """
    Exception covering case when working with unknown operator
    """

    def __init__(self, clause: FilterClause):
        """
        Constructor

        :param clause: Whole filter clause
        """
        super().__init__(
            ExceptionErrorCode.UNKNOWN_OPERATOR,
            'Unknown operator',
            exception_group=ExceptionGroup.UNSUPPORTED,
        )
        self._severity = ExceptionSeverity.info


def generate_group_filter_clause_dict(op: LogicalOperator, clauses: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Generates dictionary which matches structure of group filter clause
    """
    return {'type': FilterClauseType.GROUP.value, 'clauses': clauses, 'logical_operator': op.value}


def generate_simple_filter_clause_dict(op: SimpleFilterOperator, taxon: str, value: Any) -> Dict[str, Any]:
    """
    Generates dictionary which matches structure of taxon value filter clause
    """
    return {'type': FilterClauseType.TAXON_VALUE.value, 'operator': op.value, 'value': value, 'taxon': taxon}
