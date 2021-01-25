from abc import ABC, abstractmethod
from typing import Any, Callable, Generic, List, Optional, Set, TypeVar, cast

from sqlalchemy import (
    Float,
    Integer,
    String,
    and_,
    false,
    literal,
    literal_column,
    not_,
    or_,
    true,
)
from sqlalchemy.sql import ClauseElement, functions

from panoramic.cli.husky.common.sqlalchemy_ext import NullIf, Parentheses
from panoramic.cli.husky.core.sql_alchemy_util import safe_quote_identifier
from panoramic.cli.husky.core.taxonomy.aggregations import AggregationDefinition
from panoramic.cli.husky.core.taxonomy.enums import AggregationType, TaxonTypeEnum
from panoramic.cli.husky.core.taxonomy.models import Taxon
from panoramic.cli.husky.core.tel.evaluator.ast import (
    TelAggregationPhase,
    TelExpression,
    TelPostAggregationPhase,
)
from panoramic.cli.husky.core.tel.evaluator.context import (
    TelRootContext,
    TelValidationContext,
)
from panoramic.cli.husky.core.tel.evaluator.result import (
    TelQueryResult,
    result_with_template,
)
from panoramic.cli.husky.core.tel.result import UsedTaxonsContainer
from panoramic.cli.husky.core.tel.tel_phases import TelPhase
from panoramic.cli.husky.core.tel.types.tel_types import TelDataType, TelType
from panoramic.cli.husky.service.utils.taxon_slug_expression import TaxonExpressionStr

T = TypeVar('T')


class TelLiteral(TelExpression, ABC, Generic[T]):
    _data_type: TelDataType

    def __init__(self, context: TelRootContext, value: T):
        super().__init__(context)
        self._value = value

    def __repr__(self):
        return f'{self.__class__.__name__}({repr(self._value)})'

    @property
    def _graphviz_repr(self):
        return repr(self._value)

    def _graphviz_attribute(self, context: TelRootContext) -> str:
        return f"shape=diamond, {super()._graphviz_attribute(context)}"

    def literal_value(self, context: TelRootContext) -> Optional[Any]:
        return self._value

    def return_type(self, context: TelRootContext) -> TelType:
        return TelType(self._data_type, is_constant=True)


class TelInteger(TelLiteral[int]):
    _data_type = TelDataType.INTEGER

    def result(self, context: TelRootContext) -> TelQueryResult:
        return TelQueryResult(literal(self._value, Integer()), dialect=context.husky_dialect)


class TelFloat(TelLiteral[float]):
    _data_type = TelDataType.NUMERIC

    def result(self, context: TelRootContext) -> TelQueryResult:
        return TelQueryResult(literal(self._value, Float()), dialect=context.husky_dialect)


class TelString(TelLiteral[str]):
    _data_type = TelDataType.STRING

    def result(self, context: TelRootContext) -> TelQueryResult:
        return TelQueryResult(literal(self._value, String()), dialect=context.husky_dialect)


class TelBoolean(TelLiteral[bool]):
    _data_type = TelDataType.BOOLEAN

    def result(self, context: TelRootContext) -> TelQueryResult:
        return TelQueryResult(true() if self._value else false(), dialect=context.husky_dialect)


class TelParentheses(TelExpression):
    def __init__(self, context: TelRootContext, value: TelExpression):
        super().__init__(context)
        self._value = value

    def validate(self, context: TelValidationContext) -> TelValidationContext:
        super().validate(context)

        return self._value.validate(context)

    def result(self, context: TelRootContext) -> ClauseElement:
        result = self._value.result(context)
        sql, template = result_with_template(Parentheses, element=result)
        return result.update(sql=sql, template=template)

    def return_type(self, context: TelRootContext) -> TelType:
        return self._value.return_type(context)

    def phase(self, context: TelRootContext) -> TelPhase:
        return self._value.phase(context)

    def template_slugs(self, context: TelRootContext) -> Set[TaxonExpressionStr]:
        return self._value.template_slugs(context)

    def used_taxons(self, context: TelRootContext) -> UsedTaxonsContainer:
        return self._value.used_taxons(context)

    def return_data_sources(self, context: TelRootContext) -> Set[Optional[str]]:
        return self._value.return_data_sources(context)

    def invalid_value(self, context: TelRootContext) -> bool:
        return self._value.invalid_value(context)

    def rewrite(self, callback: Callable[[TelExpression], TelExpression], context: TelRootContext) -> TelExpression:
        return TelParentheses.copy(self, callback(self._value.rewrite(callback, context)))

    def __repr__(self):
        return f'({repr(self._value)})'

    @property
    def children(self) -> List[TelExpression]:
        return [self._value]

    def plan_phase_transitions(self, context: TelRootContext) -> TelExpression:
        return TelParentheses.copy(self, self._value.plan_phase_transitions(context))

    def aggregation_definition(self, context: TelRootContext) -> Optional[AggregationDefinition]:
        """
        Calculates the aggregation definition for this node. None, if it isn't possible to deduce the definition
        """
        return self._value.aggregation_definition(context)

    def literal_value(self, context: TelRootContext) -> Optional[Any]:
        return self._value.literal_value(context)


class TelTaxon(TelExpression, ABC):
    _name: str
    _namespace: Optional[str]
    _taxon: Optional[Taxon] = None
    _slug: TaxonExpressionStr
    _calculation_expr: Optional[TelExpression] = None

    def __init__(
        self,
        context: TelRootContext,
        name: str,
        namespace: Optional[str],
        slug: TaxonExpressionStr,
        taxon: Optional[Taxon],
        calculation_expr: Optional[TelExpression] = None,
    ):
        super().__init__(context)
        self._name = name
        self._namespace = namespace
        self._slug = slug
        self._taxon = taxon
        self._calculation_expr = calculation_expr
        if self._calculation_expr:
            self._calculation_expr.parent = self

    def validate(self, context: TelValidationContext) -> TelValidationContext:
        super().validate(context)

        if self._calculation_expr:
            self._calculation_expr.validate(context)

        return context

    def return_type(self, context: TelRootContext) -> TelType:
        if self._calculation_expr:
            return self._calculation_expr.return_type(context)
        elif self._taxon:
            return TelType.from_taxon_validation_type(self._taxon.validation_type)
        else:
            # Taxon is not set. That can happen only if the taxon is optional and out of data source scope
            # If taxon is required and out of scope, it will fail in validation step.
            return TelType(TelDataType.NONE_OPTIONAL)

    def return_data_sources(self, context: TelRootContext) -> Set[Optional[str]]:
        if self._namespace:
            return cast(Set[Optional[str]], {self._namespace}).union(
                self._calculation_expr.return_data_sources(context) if self._calculation_expr else set()
            )
        elif self._taxon and not self._calculation_expr:
            return {None}
        elif self._calculation_expr:
            return self._calculation_expr.return_data_sources(context)
        else:
            return set()

    def result(self, context: TelRootContext) -> TelQueryResult:
        taxon = self._taxon

        if self._calculation_expr:
            return self._calculation_expr.result(context)
        else:
            if context.is_benchmark and self._taxon and not self._taxon.is_dimension:
                column_name = f'comparison@{self._slug}'
            else:
                column_name = self._slug

            # Set correct result phase based on the taxon we just got.
            if taxon and TaxonTypeEnum.metric.value == taxon.taxon_type:
                sql = literal_column(safe_quote_identifier(column_name, context.husky_dialect))
            else:
                if self._namespace:
                    sql = literal_column(f'${{{self._slug}}}')
                else:
                    sql = literal_column(safe_quote_identifier(column_name, context.husky_dialect))

            return TelQueryResult(sql, dialect=context.husky_dialect)

    def phase(self, context: TelRootContext) -> TelPhase:
        if self._calculation_expr:
            phase = self._calculation_expr.phase(context)
        elif self._taxon and self._taxon.taxon_type == TaxonTypeEnum.metric.value:
            phase = TelPhase.metric_pre
        else:
            if self._namespace:
                phase = TelPhase.dimension_data_source
            else:
                phase = TelPhase.dimension

        return phase

    def template_slugs(self, context: TelRootContext) -> Set[TaxonExpressionStr]:
        if (
            not self._calculation_expr
            and self._taxon
            and self._taxon.taxon_type != TaxonTypeEnum.metric.value
            and self._namespace
        ):
            return {self._slug}
        elif self._calculation_expr:
            return self._calculation_expr.template_slugs(context)
        else:
            return set()

    @property
    def children(self) -> List[TelExpression]:
        if self._calculation_expr:
            return [self._calculation_expr]
        else:
            return []

    def rewrite(self, callback: Callable[[TelExpression], TelExpression], context: TelRootContext) -> TelExpression:
        if self._calculation_expr:
            return self.copy(
                self,
                self._name,
                self._namespace,
                self._slug,
                self._taxon,
                callback(self._calculation_expr.rewrite(callback, context)),
            )

        return self

    def plan_phase_transitions(self, context: TelRootContext) -> TelExpression:
        # Ensure taxon calculation phase transitions are propagated to this taxon node.
        if self._calculation_expr and not self._calculation_expr.invalid_value(context):
            return self.copy(
                self,
                self._name,
                self._namespace,
                self._slug,
                self._taxon,
                self._calculation_expr.plan_phase_transitions(context),
            )

        return self

    def _allowed(self, context: TelRootContext) -> bool:
        return (self._namespace and context.is_data_source_allowed(self._namespace)) or not self._namespace

    def invalid_value(self, context: TelRootContext) -> bool:
        if self._calculation_expr:
            return self._calculation_expr.invalid_value(context)
        else:
            return not self._taxon

    def used_taxons(self, context: TelRootContext) -> UsedTaxonsContainer:
        if self._calculation_expr:
            return self._calculation_expr.used_taxons(context)

        return UsedTaxonsContainer()

    def aggregation_definition(self, context: TelRootContext) -> Optional[AggregationDefinition]:
        """
        Calculates the aggregation definition for this node. None, if it isn't possible to deduce the definition
        """

        # there was calculation expression so let's deduce the aggregation type from it
        if self._calculation_expr:
            return self._calculation_expr.aggregation_definition(context)
        else:
            # taxon is defined and we also want to enforce its aggregation type
            if self._taxon and self._taxon.aggregation:
                return self._taxon.aggregation

            # otherwise, no aggregation type is set for metric & group_by aggregation is set for dimensions
            agg_type = (
                AggregationType.not_set
                if self._taxon is None or self._taxon.taxon_type == 'metric'
                else AggregationType.group_by
            )

            return AggregationDefinition(type=agg_type)

    def literal_value(self, context: TelRootContext) -> Optional[Any]:
        if self._calculation_expr:
            return self._calculation_expr.literal_value(context)

        return None


class TelOptionalTaxon(TelTaxon):
    def used_taxons(self, context: TelRootContext) -> UsedTaxonsContainer:
        result = UsedTaxonsContainer.optional(self._taxon) if self._taxon else UsedTaxonsContainer()
        return UsedTaxonsContainer.merge(super().used_taxons(context), result)

    def invalid_value(self, context: TelRootContext) -> bool:
        return (not self._allowed(context)) or super().invalid_value(context)

    def __repr__(self):
        return f'?{self._slug}'


class TelRequiredTaxon(TelTaxon):
    def validate(self, context: TelValidationContext) -> TelValidationContext:
        super().validate(context)

        if not self._allowed(context.root_context):
            context.with_error(f'Missing required taxon "{self._slug}"', location=self.location)

        if not self._taxon:
            context.with_error(f'Taxon "{self._slug}" not found', location=self.location)

        return context

    def used_taxons(self, context: TelRootContext) -> UsedTaxonsContainer:
        result = UsedTaxonsContainer.required(self._taxon) if self._taxon else UsedTaxonsContainer()
        return UsedTaxonsContainer.merge(super().used_taxons(context), result)

    def __repr__(self):
        return self._slug


class TelBinaryOp(TelExpression, ABC):
    _op: str
    _left: TelExpression
    _right: TelExpression
    _left_invalid: bool
    _right_invalid: bool

    def __init__(self, context: TelRootContext, op: str, left: TelExpression, right: TelExpression):
        super().__init__(context)
        self._op = op

        self._left = left
        self._right = right

        self._left_invalid = self._left.invalid_value(context)
        self._right_invalid = self._right.invalid_value(context)

    def validate(self, context: TelValidationContext) -> TelValidationContext:
        super().validate(context)

        self._left.validate(context)
        self._right.validate(context)

        return context

    def used_taxons(self, context: TelRootContext) -> UsedTaxonsContainer:
        if self._left_invalid and self._right_invalid:
            return UsedTaxonsContainer()
        elif self._left_invalid:
            return self._right.used_taxons(context)
        elif self._right_invalid:
            return self._left.used_taxons(context)
        else:
            return UsedTaxonsContainer.merge(self._left.used_taxons(context), self._right.used_taxons(context))

    def return_data_sources(self, context: TelRootContext) -> Set[Optional[str]]:
        return self._left.return_data_sources(context) | self._right.return_data_sources(context)

    def phase(self, context: TelRootContext) -> TelPhase:
        return TelPhase.max([self._left.phase(context), self._right.phase(context)])

    def template_slugs(self, context: TelRootContext) -> Set[TaxonExpressionStr]:
        return self._left.template_slugs(context) | self._right.template_slugs(context)

    def rewrite(self, callback: Callable[[TelExpression], TelExpression], context: TelRootContext) -> TelExpression:
        result = cast(
            TelBinaryOp,
            self.__class__.copy(
                self,
                self._op,
                callback(self._left.rewrite(callback, context)),
                callback(self._right.rewrite(callback, context)),
            ),
        )

        result._left_invalid = self._left_invalid
        result._right_invalid = self._right_invalid

        return result

    def __repr__(self):
        return f'{repr(self._left)} {self._op} {repr(self._right)}'

    @property
    def _graphviz_repr(self):
        left_label = repr(self._left) if isinstance(self._left, TelTaxon) else self._left._node_id
        right_label = repr(self._right) if isinstance(self._right, TelTaxon) else self._right._node_id
        return f'{left_label} {self._op} {right_label}'

    @property
    def children(self) -> List[TelExpression]:
        return [self._left, self._right]

    def aggregation_definition(self, context: TelRootContext) -> Optional[AggregationDefinition]:
        """
        Calculates the aggregation definition for this node. None, if it isn't possible to deduce the definition
        """
        agg_definitions = [self._left.aggregation_definition(context), self._right.aggregation_definition(context)]

        return AggregationDefinition.common_defined_definition(agg_definitions)

    @property
    def op(self):
        return self._op

    @property
    def left(self):
        return self._left

    @property
    def right(self):
        return self._right


class TelNumericOp(TelBinaryOp, ABC):
    _op_description: str

    def validate(self, context: TelValidationContext) -> TelValidationContext:
        super().validate(context)

        if context.has_errors:
            return context

        if not self._left_invalid:
            left_return_type = self._left.return_type(context.root_context)
            if not left_return_type.is_number():
                context.with_error(
                    f'Operand 1 in {self._op_description} expression must be of type: number', self._left.location
                )

        if not self._right_invalid:
            right_return_type = self._right.return_type(context.root_context)
            if not right_return_type.is_number():
                context.with_error(
                    f'Operand 2 in {self._op_description} expression must be of type: number', self._right.location
                )

        return context

    def return_type(self, context: TelRootContext) -> TelType:
        types = []

        if not self._left_invalid:
            types.append(self._left.return_type(context))

        if not self._right_invalid:
            types.append(self._right.return_type(context))

        if not types:
            types.append(TelType(TelDataType.ANY))

        return TelType.return_common_type(types)

    def phase(self, context: TelRootContext) -> TelPhase:
        if self._is_post_aggregation(context):
            return TelPhase.metric_post
        else:
            return TelPhase.metric_pre

    def result(self, context: TelRootContext) -> TelQueryResult:
        left_result = self._left.result(context)
        right_result = self._right.result(context)

        sql, template = result_with_template(self._sql, left=left_result, right=right_result)

        return TelQueryResult.merge(
            sql,
            context.husky_dialect,
            left_result if not self._left_invalid else None,
            right_result if not self._right_invalid else None,
            template=template,
        )

    @abstractmethod
    def _is_post_aggregation(self, context: TelRootContext) -> bool:
        pass

    @abstractmethod
    def _sql(self, left: ClauseElement, right: ClauseElement) -> ClauseElement:
        pass

    def plan_phase_transitions(self, context: TelRootContext) -> TelExpression:
        left = self._left
        right = self._right

        if self._is_post_aggregation(context):
            # if this node is already in post aggregation phase, left and right side must be taken to this phase as well
            if TelPhase.metric_post != left.phase(context):
                left = TelPostAggregationPhase.copy(
                    self._left, left.plan_phase_transitions(context), left.aggregation_definition(context)
                )
            if TelPhase.metric_post != right.phase(context):
                right = TelPostAggregationPhase.copy(
                    self._right, right.plan_phase_transitions(context), right.aggregation_definition(context)
                )

        result = cast(
            TelNumericOp,
            self.__class__.copy(
                self, self._op, left.plan_phase_transitions(context), right.plan_phase_transitions(context)
            ),
        )

        result._left_invalid = self._left_invalid
        result._right_invalid = self._right_invalid

        return result

    def rewrite(self, callback: Callable[[TelExpression], TelExpression], context: TelRootContext) -> TelExpression:
        literal_value = self.literal_value(context)
        if literal_value is not None:
            return (
                TelInteger.copy(self, literal_value)
                if isinstance(literal_value, int)
                else TelFloat.copy(self, literal_value)
            )

        return super().rewrite(callback, context)


class TelStrictNumericOp(TelNumericOp, ABC):
    def _is_post_aggregation(self, context: TelRootContext):
        return (
            # That is when one of the operands must be post
            TelPhase.metric_post
            in [
                phase
                for phase in [
                    self._left.phase(context) if not self._left_invalid else None,
                    self._right.phase(context) if not self._right_invalid else None,
                ]
                if phase
            ]
            or (self._left.return_type(context).is_constant and self._right.return_type(context).is_constant)
            or (
                self._left.used_taxons(context).has_some() and self._right.used_taxons(context).has_some()
            )  #  Or both used some taxons
        )

    def invalid_value(self, context: TelRootContext) -> bool:
        return self._left_invalid or self._right_invalid


class TelMultiplication(TelStrictNumericOp):
    _op_description = 'multiplication'

    def literal_value(self, context: TelRootContext) -> Optional[Any]:
        left_value = self._left.literal_value(context)
        right_value = self._right.literal_value(context)

        if left_value is not None and right_value is not None:
            return left_value * right_value

        return None

    def _sql(self, left: ClauseElement, right: ClauseElement) -> ClauseElement:
        return left * right


class TelDivision(TelStrictNumericOp):
    _op_description = 'division'

    def _sql(self, left: ClauseElement, right: ClauseElement) -> ClauseElement:
        return left / NullIf(right, literal(0, Integer()))

    def literal_value(self, context: TelRootContext) -> Optional[Any]:
        left_value = self._left.literal_value(context)
        right_value = self._right.literal_value(context)

        if left_value is not None and right_value is not None and int(right_value) != 0:
            return left_value / right_value

        return None


class TelTolerantNumericOp(TelNumericOp, ABC):
    def _is_post_aggregation(self, context: TelRootContext):
        return (
            #  Must be post if some operand must be post
            TelPhase.metric_post
            in [
                phase
                for phase in [
                    self._left.phase(context) if not self._left_invalid else None,
                    self._right.phase(context) if not self._right_invalid else None,
                ]
                if phase
            ]
            # Or if some operand does not use any taxon (adding constants must be always done post) and is not 0
            # coz adding any other constant than 0 must be done after aggregation.
            or (not self._left_invalid and not self._left.used_taxons(context).has_some())
            or (not self._right_invalid and not self._right.used_taxons(context).has_some())
        )


class TelAddition(TelTolerantNumericOp):
    _op_description = 'addition'

    def _sql(self, left: ClauseElement, right: ClauseElement) -> ClauseElement:
        return functions.coalesce(left, literal(0, Integer())) + functions.coalesce(right, literal(0, Integer()))

    def invalid_value(self, context: TelRootContext) -> bool:
        return self._left_invalid and self._right_invalid

    def rewrite(self, callback: Callable[[TelExpression], TelExpression], context: TelRootContext) -> TelExpression:
        if self._left_invalid:
            return self._right.rewrite(callback, context)
        elif self._right_invalid:
            return self._left.rewrite(callback, context)

        return super().rewrite(callback, context)

    def literal_value(self, context: TelRootContext) -> Optional[Any]:
        left_value = self._left.literal_value(context)
        right_value = self._right.literal_value(context)

        if left_value is not None and right_value is not None:
            return left_value + right_value

        return None


class TelSubtraction(TelTolerantNumericOp):
    _op_description = 'subtraction'

    def _sql(self, left: ClauseElement, right: ClauseElement) -> ClauseElement:
        return functions.coalesce(left, literal(0, Integer())) - functions.coalesce(right, literal(0, Integer()))

    def invalid_value(self, context: TelRootContext) -> bool:
        return self._left_invalid

    def rewrite(self, callback: Callable[[TelExpression], TelExpression], context: TelRootContext) -> TelExpression:
        if self._right_invalid and not self._left_invalid:
            return self._left.rewrite(callback, context)

        return super().rewrite(callback, context)

    def literal_value(self, context: TelRootContext) -> Optional[Any]:
        left_value = self._left.literal_value(context)
        right_value = self._right.literal_value(context)

        if left_value is not None and right_value is not None:
            return left_value - right_value

        return None


class TelLogicalOperation(TelBinaryOp):
    def validate(self, context: TelValidationContext) -> TelValidationContext:
        super().validate(context)

        if not TelType.are_compatible_data_types(
            [self._left.return_type(context.root_context), self._right.return_type(context.root_context)]
        ):
            context.with_error('Operands in logical expression must have compatible data types', location=self.location)

        return context

    def result(self, context: TelRootContext) -> TelQueryResult:
        left_result = self._left.result(context)
        right_result = self._right.result(context)

        sql, template = result_with_template(self._sql, left=left_result, right=right_result)

        return TelQueryResult.merge(sql, context.husky_dialect, left_result, right_result, template=template)

    def _sql(self, left: ClauseElement, right: ClauseElement) -> ClauseElement:
        if self._op == 'AND':
            sql = and_(left, right)
        elif self._op == 'OR':
            sql = or_(left, right)
        elif self._op == '=':
            sql = left == right
        elif self._op == '<':
            sql = left < right
        elif self._op == '<=':
            sql = left <= right
        elif self._op == '>':
            sql = left > right
        elif self._op == '>=':
            sql = left >= right
        else:  # self._op == '!=':
            sql = left != right

        return sql

    def return_type(self, context: TelRootContext) -> TelType:
        types = []

        if not self._left_invalid:
            types.append(self._left.return_type(context))
        if not self._right_invalid:
            types.append(self._right.return_type(context))

        return TelType.return_common_type(types).copy(data_type=TelDataType.BOOLEAN)

    def phase(self, context: TelRootContext) -> TelPhase:
        if TelPhase.metric_post in [
            phase
            for phase in [
                self._left.phase(context) if not self._left_invalid else None,
                self._right.phase(context) if not self._right_invalid else None,
            ]
            if phase
        ]:
            return TelPhase.metric_post

        return super().phase(context)

    def invalid_value(self, context: TelRootContext) -> bool:
        return self._left_invalid or self._right_invalid

    def plan_phase_transitions(self, context: TelRootContext) -> TelExpression:
        left = self._left
        right = self._right

        left_phase = left.phase(context)
        right_phase = right.phase(context)
        parent_phase = self.parent.phase(context) if self.parent else None

        # Ensure left and right side nodes are in the same phase (aggregation) as the parent node.
        if parent_phase and parent_phase.is_metric():
            if left_phase.is_dimension():
                left = TelAggregationPhase.copy(
                    self._left, left.plan_phase_transitions(context), left.aggregation_definition(context)
                ).plan_phase_transitions(context)

            if right_phase.is_dimension():
                right = TelAggregationPhase.copy(
                    self._right, right.plan_phase_transitions(context), right.aggregation_definition(context)
                ).plan_phase_transitions(context)

        left_phase = left.phase(context)
        right_phase = right.phase(context)

        # And if the parent is already in post aggregation, lift the child nodes to that phase as well
        if TelPhase.metric_post == parent_phase:
            if TelPhase.metric_pre == left_phase:
                left = TelPostAggregationPhase.copy(self._left, left, left.aggregation_definition(context))

            if TelPhase.metric_pre == right_phase:
                right = TelPostAggregationPhase.copy(self._left, right, right.aggregation_definition(context))

        result = cast(
            TelLogicalOperation,
            TelLogicalOperation.copy(
                self, self._op, left.plan_phase_transitions(context), right.plan_phase_transitions(context)
            ),
        )

        result._left_invalid = self._left_invalid
        result._right_invalid = self._right_invalid

        return result

    def aggregation_definition(self, context: TelRootContext) -> Optional[AggregationDefinition]:
        """
        Calculates the aggregation definition for this node. None, if it isn't possible to deduce the definition
        """
        return AggregationDefinition(type=AggregationType.group_by)

    def literal_value(self, context: TelRootContext) -> Optional[Any]:
        left_value = self._left.literal_value(context)
        right_value = self._right.literal_value(context)

        if left_value is not None and right_value is not None:
            if self._op == 'AND':
                return left_value and right_value
            elif self._op == 'OR':
                return left_value or right_value
            elif self._op == '=':
                return left_value == right_value
            elif self._op == '<':
                return left_value < right_value
            elif self._op == '<=':
                return left_value <= right_value
            elif self._op == '>':
                return left_value > right_value
            elif self._op == '>=':
                return left_value >= right_value
            else:  # self._op == '!=':
                return left_value != right_value

        return None

    def rewrite(self, callback: Callable[[TelExpression], TelExpression], context: TelRootContext) -> TelExpression:
        literal_value = self.literal_value(context)
        if literal_value is not None:
            if isinstance(literal_value, bool):
                return TelBoolean.copy(self, literal_value)
            elif isinstance(literal_value, int):
                return TelInteger.copy(self, literal_value)
            elif isinstance(literal_value, float):
                return TelFloat.copy(self, literal_value)
            elif isinstance(literal_value, str):
                return TelString.copy(self, literal_value)

        return super().rewrite(callback, context)


class TelUnaryOp(TelExpression, ABC):
    def __init__(self, context: TelRootContext, expression: TelExpression):
        super().__init__(context)
        self._expression = expression

    def validate(self, context: TelValidationContext) -> TelValidationContext:
        super().validate(context)

        return self._expression.validate(context)

    def rewrite(self, callback: Callable[[TelExpression], TelExpression], context: TelRootContext) -> TelExpression:
        return self.__class__.copy(self, callback(self._expression.rewrite(callback, context)))

    def plan_phase_transitions(self, context: TelRootContext) -> TelExpression:
        return self.__class__.copy(self, self._expression.plan_phase_transitions(context))

    def phase(self, context: TelRootContext) -> TelPhase:
        return self._expression.phase(context)

    def template_slugs(self, context: TelRootContext) -> Set[TaxonExpressionStr]:
        return self._expression.template_slugs(context)

    def used_taxons(self, context: TelRootContext) -> UsedTaxonsContainer:
        return self._expression.used_taxons(context)

    def return_data_sources(self, context: TelRootContext) -> Set[Optional[str]]:
        return self._expression.return_data_sources(context)

    def invalid_value(self, context: TelRootContext) -> bool:
        return self._expression.invalid_value(context)

    @property
    def children(self) -> List[TelExpression]:
        return [self._expression]

    def aggregation_definition(self, context: TelRootContext) -> Optional[AggregationDefinition]:
        """
        Calculates the aggregation definition for this node. None, if it isn't possible to deduce the definition
        """
        return AggregationDefinition(type=AggregationType.group_by)


class TelNot(TelUnaryOp):
    def validate(self, context: TelValidationContext) -> TelValidationContext:
        super().validate(context)

        if not self._expression.return_type(context.root_context).is_boolean():
            context.with_error('Operand in not expression must be of type: boolean', location=self.location)

        return context

    def result(self, context: TelRootContext) -> TelQueryResult:
        result = self._expression.result(context)
        sql, template = result_with_template(not_, clause=result)
        return result.update(sql=sql, template=template)

    def return_type(self, context: TelRootContext) -> TelType:
        return self._expression.return_type(context)

    def __repr__(self) -> str:
        return f'NOT {repr(self._expression)}'

    @property
    def _graphviz_repr(self):
        return f'NOT {self._expression._node_id}'

    def literal_value(self, context: TelRootContext) -> Optional[Any]:
        expr_value = self._expression.literal_value(context)
        if expr_value is not None:
            return not expr_value

        return None


class TelIsNull(TelUnaryOp):
    def result(self, context: TelRootContext) -> TelQueryResult:
        result = self._expression.result(context)
        sql, template = result_with_template(lambda clause: clause == None, clause=result)
        return result.update(sql=sql, template=template)

    def return_type(self, context: TelRootContext) -> TelType:
        return self._expression.return_type(context).copy(data_type=TelDataType.BOOLEAN)

    def __repr__(self) -> str:
        return f'{repr(self._expression)} IS NULL'

    @property
    def _graphviz_repr(self):
        return f'{self._expression._node_id} IS NULL'

    def literal_value(self, context: TelRootContext) -> Optional[Any]:
        return None


class TelIsNotNull(TelUnaryOp):
    def result(self, context: TelRootContext) -> TelQueryResult:
        result = self._expression.result(context)
        sql, template = result_with_template(lambda clause: clause != None, clause=result)
        return result.update(sql=sql, template=template)

    def return_type(self, context: TelRootContext) -> TelType:
        return self._expression.return_type(context).copy(data_type=TelDataType.BOOLEAN)

    def __repr__(self) -> str:
        return f'{repr(self._expression)} IS NOT NULL'

    @property
    def _graphviz_repr(self):
        return f'{self._expression._node_id} IS NOT NULL'

    def literal_value(self, context: TelRootContext) -> Optional[Any]:
        return None
