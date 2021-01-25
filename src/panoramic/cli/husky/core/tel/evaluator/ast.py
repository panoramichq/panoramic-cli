from abc import ABC, abstractmethod
from operator import itemgetter
from typing import Any, Callable, Dict, List, Optional, Set, TypeVar, cast

from sqlalchemy import literal_column, null

from panoramic.cli.husky.core.sql_alchemy_util import (
    compile_query,
    safe_quote_identifier,
)
from panoramic.cli.husky.core.taxonomy.aggregations import AggregationDefinition
from panoramic.cli.husky.core.taxonomy.enums import AggregationType, TaxonTypeEnum
from panoramic.cli.husky.core.tel.evaluator.context import (
    ParserLocation,
    TelRootContext,
    TelValidationContext,
)
from panoramic.cli.husky.core.tel.evaluator.result import TelQueryResult
from panoramic.cli.husky.core.tel.result import (
    PreFormula,
    TelPhase,
    UsedTaxonsContainer,
)
from panoramic.cli.husky.core.tel.sql_formula import SqlFormulaTemplate, SqlTemplate
from panoramic.cli.husky.core.tel.types.tel_types import TelType
from panoramic.cli.husky.service.utils.taxon_slug_expression import TaxonExpressionStr

T = TypeVar('T')


class TelExpression(ABC):
    """
    Base class for all AST nodes. Contains the API definition each node must expose, and other utility functions,
    for debugging, instantiating and cloning AST nodes.
    """

    _location: ParserLocation
    _context: TelRootContext
    _parent: Optional['TelExpression'] = None
    _node_id: int

    @abstractmethod
    def __init__(self, context: TelRootContext, *args, **kwargs):
        """
        This is just a stub for the mypy. Implementing class must provide a constructor, where the first argument is
        TelRootContext, but they don't need to store it locally. It is always accessible as a local private variable
        _context. This is done by method `new`, but it also must be passed to the constructor, if the constructor needs
        to use it.
        """
        self._node_id = context.next_node_id()

    def __eq__(self, o: object) -> bool:
        return isinstance(o, TelExpression) and self._node_id == o._node_id

    def __hash__(self) -> int:
        return hash(self._node_id)

    def validate(self, context: TelValidationContext) -> TelValidationContext:
        """
        Step 1: Semantic errors, such as invalid taxons, incompatible aggregation types of expressions,
        or invalid phase combinations are collected in this phase and reported to the user later.
        """
        return context

    def rewrite(
        self, callback: Callable[['TelExpression'], 'TelExpression'], context: TelRootContext
    ) -> 'TelExpression':
        """
        Step 2: optimize/rewrite the expression. This steps allows either modification of local variables, after
        validation has already been performed, but more typically this can return a completely new TelExpression,
        and in this way rewrite the final expression, so that for example adding zero with something can be rewritten
        to just that something, avoiding producing unnecessary/suboptimal pieces of SQL queries.
        Callback is meant to be applied on all children of the newly created expression.
        """
        return self

    def plan_phase_transitions(self, context: TelRootContext) -> 'TelExpression':
        """
        Step 3: add phase transition nodes as needed. This returns a new expression in the correct phase, meaning that
        for example arguments of the current function can be moved to higher phase, by using one of TelPhaseTransition
        implementations, so that the final node already has its children in the correct phase, when rendering the final
        result.
        """
        return self

    @property
    def parent(self) -> Optional['TelExpression']:
        return self._parent

    @parent.setter
    def parent(self, parent):
        self._parent = parent

    @property
    def children(self) -> List['TelExpression']:
        return []

    @abstractmethod
    def result(self, context: TelRootContext) -> TelQueryResult:
        """
        Resulting SQL and formulas
        """
        pass

    def phase(self, context: TelRootContext) -> TelPhase:
        """
        Return phase of the expression
        """
        return TelPhase.any

    def template_slugs(self, context: TelRootContext) -> Set[TaxonExpressionStr]:
        """
        Set of taxons used in a template formula. To be used when rendering the template.
        """
        return set()

    @abstractmethod
    def return_type(self, context: TelRootContext) -> TelType:
        """This must be called after initialize has been called"""
        pass

    def used_taxons(self, context: TelRootContext) -> UsedTaxonsContainer:
        """This must be called after initialize has been called"""
        return UsedTaxonsContainer()

    def return_data_sources(self, context: TelRootContext) -> Set[Optional[str]]:
        """This must be called after initialize has been called"""
        return set()

    def invalid_value(self, context: TelRootContext) -> bool:
        """
        Flag if the result has no valid value, typically when referencing to taxons from which none are available.
        This must be called after initialize has been called
        """
        return False

    def fold(
        self, callback: Callable[['TelExpression', T, TelRootContext], T], initial: T, context: TelRootContext
    ) -> T:
        """
        Execute callback on self including all children and return accumulated value.
        """
        acc = callback(self, initial, context)

        for child in self.children:
            acc = child.fold(callback, acc, context)

        return acc

    @abstractmethod
    def literal_value(self, context: TelRootContext) -> Optional[Any]:
        """
        Return a literal value of the node, provided that `return_type()` of that node returns a type
        with `is_constant` equal to `True`.

        :return: any value, representing the literal value of the node, or `None` if the node is not constant.
        """
        pass

    @property
    def location(self) -> ParserLocation:
        return self._location

    @abstractmethod
    def __repr__(self):
        pass

    @property
    def _graphviz_repr(self):
        return repr(self)

    def _graphviz_name(self):
        return f'"{self._node_id}: {self._graphviz_repr}"'

    def to_graphviz(self, context: TelRootContext) -> str:
        """
        Produce Graphviz digraph with a node representation as a graph.
        """
        result = ''

        for child in self.children:
            result += f'{child._graphviz_name()} -> {self._graphviz_name()};'
            result += child.to_graphviz(context)

        return result

    def graphviz_description(self, context: TelRootContext) -> Dict[int, str]:
        invalid = self.invalid_value(context)
        return_type = self.return_type(context)
        agg_definition = self.aggregation_definition(context)

        result = {
            self._node_id: f'<tr>'
            f'<td bgcolor="{self._graphviz_node_color(context)}"><b>{self._node_id}</b></td>'
            f'<td bgcolor="{self._graphviz_node_color(context)}">{self.__class__.__name__}</td>'
            f'<td bgcolor="{self._graphviz_node_color(context)}">{self.phase(context).name}{",&nbsp;<b>invalid</b>" if invalid else ""}</td>'
            f'<td bgcolor="{self._graphviz_node_color(context)}">{return_type.data_type.name}{"&nbsp;(constant)" if return_type.is_constant else ""}</td>'
            f'<td bgcolor="{self._graphviz_node_color(context)}">{agg_definition.type.name if agg_definition else ""}</td>'
            f'</tr>\n'
        }

        for child in self.children:
            result.update(child.graphviz_description(context))

        return result

    def _graphviz_attribute(self, context: TelRootContext) -> str:
        return f'style=filled, fillcolor="{self._graphviz_node_color(context)}"'

    def _graphviz_node_color(self, context: TelRootContext):
        phase = self.phase(context)
        invalid = self.invalid_value(context)

        if invalid:
            color = "#F26430"
        else:
            color = phase.graphviz_fillcolor

        return color

    def graphviz_attributes(self, context: TelRootContext) -> Dict['TelExpression', str]:
        result = {self: self._graphviz_attribute(context)}

        for child in self.children:
            result.update(child.graphviz_attributes(context))

        return result

    @classmethod
    def copy(cls, other: 'TelExpression', *args, **kwargs) -> 'TelExpression':
        """
        Useful for cloning AST nodes and it sets the parent and parser context to the same values as the argument
        provided here.
        """
        result = cls.new(other._location, other._context, *args, **kwargs)

        if other.parent:
            result._parent = other._parent

        return result

    @classmethod
    def new(cls, location: ParserLocation, context: TelRootContext, *args, **kwargs) -> 'TelExpression':
        """
        Create a new instance of the TelExpression implementation class, and set up local variables as needed, and
        the children/parent relationships between nodes.
        """
        result = cls(context, *args, **kwargs)
        result._location = location
        result._context = context

        for child in result.children:
            child._parent = result

        return result

    def aggregation_definition(self, context: TelRootContext) -> Optional[AggregationDefinition]:
        """
        Calculates the aggregation definition for this node. None, if it isn't possible to deduce the definition
        """
        return AggregationDefinition(type=AggregationType.not_set)


class TelPhaseTransition(TelExpression, ABC):
    """
    Base class for phase transition node.
    """

    _cached_label: Optional[str] = None
    _value: TelExpression
    _phase: TelPhase

    def __init__(self, context: TelRootContext, value: TelExpression):
        """
        Constructor common for all phase transition nodes.
        :param value: the node to be phase-transitioned
        """
        super().__init__(context)

        self._value = value

    def validate(self, context: TelValidationContext) -> TelValidationContext:
        return self._value.validate(context)

    def return_type(self, context: TelRootContext) -> TelType:
        return self._value.return_type(context)

    def template_slugs(self, context: TelRootContext) -> Set[TaxonExpressionStr]:
        return self._value.template_slugs(context)

    def used_taxons(self, context: TelRootContext) -> UsedTaxonsContainer:
        return self._value.used_taxons(context)

    def return_data_sources(self, context: TelRootContext) -> Set[Optional[str]]:
        return self._value.return_data_sources(context)

    def invalid_value(self, context: TelRootContext) -> bool:
        return self._value.invalid_value(context)

    def __repr__(self):
        return f'{repr(self._value)} >>> {self._phase.name}'

    @property
    def _graphviz_repr(self):
        return self._phase.name

    @property
    def children(self) -> List[TelExpression]:
        return [self._value]

    def phase(self, context: TelRootContext) -> TelPhase:
        return self._phase

    def aggregation_definition(self, context: TelRootContext) -> Optional[AggregationDefinition]:
        """
        Calculates the aggregation definition for this node. None, if it isn't possible to deduce the definition
        """

        return self._value.aggregation_definition(context)

    def _graphviz_attribute(self, context: TelRootContext) -> str:
        return f"shape=rarrow, {super()._graphviz_attribute(context)}"

    def literal_value(self, context: TelRootContext) -> Optional[Any]:
        return self._value.literal_value(context)


class TelDimensionPhase(TelPhaseTransition):
    """
    This phase transition takes its node to the dimension node, if it is currently in a lower one
    (dimension_data_source or any).
    SqlFormulaTemplate is appended to the result.
    """

    _phase = TelPhase.dimension

    def result(self, context: TelRootContext) -> TelQueryResult:
        result = self._value.result(context)

        data_source_formula_templates = []

        value_phase = self._value.phase(context)
        if value_phase == self._phase:
            return result

        if value_phase in [TelPhase.dimension_data_source, TelPhase.any]:
            if self.used_taxons(context).has_some():
                if not self._cached_label:
                    self._cached_label = context.new_label

                assert 1 == len(self._value.return_data_sources(context))

                data_source = cast(
                    str, next(ds for ds in list(self._value.return_data_sources(context)) if ds is not None)
                )

                sql = literal_column(safe_quote_identifier(self._cached_label, context.husky_dialect))
                data_source_formula_templates.append(
                    SqlFormulaTemplate(
                        SqlTemplate(compile_query(result.sql, context.husky_dialect)),
                        cast(str, self._cached_label),
                        data_source,
                        cast(Set[str], self._value.template_slugs(context)),
                    )
                )
                template = sql
            else:
                sql = result.sql
                template = result.template

            label = self._cached_label or result.label

            if not self._value.invalid_value(context):
                return TelQueryResult(
                    sql=sql,
                    dialect=context.husky_dialect,
                    aggregations=result.aggregations,
                    dimension_formulas=result.dimension_formulas,
                    data_source_formula_templates=data_source_formula_templates + result.data_source_formula_templates,
                    label=label,
                    exclude_slugs=result.exclude_slugs,
                    template=template,
                )
            else:
                return TelQueryResult(
                    sql,
                    dialect=context.husky_dialect,
                    data_source_formula_templates=data_source_formula_templates,
                    label=label,
                )
        else:
            raise RuntimeError(f'Cannot move to {self._phase} phase from {value_phase}')


class TelAggregationPhase(TelPhaseTransition):
    """
    This phase transition takes its node to the aggregation node, if it is currently in a lower one
    (dimension or any).
    PreFormula is appended to dimension_formulas of the result.
    """

    _phase = TelPhase.metric_pre

    def __init__(
        self,
        context: TelRootContext,
        value: TelExpression,
        aggregation: Optional[AggregationDefinition] = None,
        label: Optional[str] = None,
    ):
        super().__init__(context, value)
        self._aggregation = aggregation or AggregationDefinition(type=AggregationType.not_set)
        self._label = label

    def result(self, context: TelRootContext) -> TelQueryResult:
        result = self._value.result(context)

        dimension_formulas = []

        value_phase = self._value.phase(context)
        if value_phase == self._phase:
            return result

        if value_phase in [TelPhase.dimension, TelPhase.any]:
            if self.used_taxons(context).has_some():
                if not self._label and not self._cached_label:
                    self._cached_label = context.new_label

                dimension_formulas.append(
                    PreFormula(
                        result.sql,
                        cast(str, self._label or self._cached_label),
                        # no aggregations are performed in Dimension Builder
                        AggregationDefinition(type=AggregationType.not_set),
                    )
                )
                sql = literal_column(safe_quote_identifier(self._label or self._cached_label, context.husky_dialect))
                template = sql
            else:
                sql = result.sql
                template = result.template

            label = self._label or self._cached_label or result.label

            if not self._value.invalid_value(context):
                return TelQueryResult(
                    sql=sql,
                    dialect=context.husky_dialect,
                    aggregations=result.aggregations,
                    dimension_formulas=dimension_formulas + result.dimension_formulas,
                    data_source_formula_templates=result.data_source_formula_templates,
                    override_mappings=result.override_mappings,
                    label=label,
                    exclude_slugs=result.exclude_slugs,
                    template=template,
                )
            else:
                return TelQueryResult(
                    sql, dialect=context.husky_dialect, dimension_formulas=dimension_formulas, label=label
                )
        else:
            raise RuntimeError(f'Cannot move to {self._phase} phase from {value_phase}')

    def plan_phase_transitions(self, context: TelRootContext) -> TelExpression:
        if TelPhase.dimension_data_source == self._value.phase(context):
            return TelAggregationPhase.copy(
                self,
                TelDimensionPhase.copy(self, self._value.plan_phase_transitions(context)).plan_phase_transitions(
                    context
                ),
                label=self._label,
            )

        return self

    def aggregation_definition(self, context: TelRootContext) -> Optional[AggregationDefinition]:
        """
        Calculates the aggregation definition for this node. None, if it isn't possible to deduce the definition
        """

        return self._aggregation


class TelPostAggregationPhase(TelPhaseTransition):
    """
    This phase transition takes its node to the post aggregation node, if it is currently in a lower one
    (metric_pre or any).
    PreFormula is appended to aggregations of the result.
    """

    _phase = TelPhase.metric_post

    def __init__(
        self,
        context: TelRootContext,
        value: TelExpression,
        aggregation: Optional[AggregationDefinition] = None,
        label: Optional[str] = None,
    ):
        super().__init__(context, value)
        self._aggregation = aggregation or AggregationDefinition(type=AggregationType.sum)
        self._label = label

    def result(self, context: TelRootContext) -> TelQueryResult:
        result = self._value.result(context)

        aggregations = []

        value_phase = self._value.phase(context)
        if value_phase == self._phase:
            return result

        if value_phase in [TelPhase.metric_pre, TelPhase.any]:
            if self.used_taxons(context).has_some():
                if not self._label and not self._cached_label:
                    self._cached_label = context.new_label

                label = cast(str, self._label or self._cached_label)

                aggregations.append(PreFormula(result.sql, cast(str, label), self.aggregation_definition(context)))
                sql = literal_column(safe_quote_identifier(label, context.husky_dialect))
                template = sql
            else:
                sql = result.sql
                template = result.template

            label = cast(str, self._label or self._cached_label or result.label)

            if not self._value.invalid_value(context):
                return TelQueryResult(
                    sql=sql,
                    dialect=context.husky_dialect,
                    aggregations=aggregations + result.aggregations,
                    dimension_formulas=result.dimension_formulas,
                    data_source_formula_templates=result.data_source_formula_templates,
                    label=label,
                    override_mappings=result.override_mappings,
                    exclude_slugs=result.exclude_slugs,
                    template=template,
                )
            else:
                return TelQueryResult(sql, dialect=context.husky_dialect, aggregations=aggregations, label=label)
        else:
            raise RuntimeError(f'Cannot move to {self._phase} phase from {value_phase}')

    def aggregation_definition(self, context: TelRootContext) -> Optional[AggregationDefinition]:
        """
        Calculates the aggregation definition for this node. None, if it isn't possible to deduce the definition
        """

        return self._aggregation


class TelRoot(TelExpression):
    """
    The root of all Tel AST trees. It ensures that the final result is always in the final phase and transitions nodes
    to the final phase if necessary.
    """

    _value: TelExpression

    def __init__(self, context: TelRootContext, value: TelExpression):
        super().__init__(context)
        self._value = value

    def validate(self, context: TelValidationContext) -> TelValidationContext:
        if self._value.validate(context).has_errors:
            return context

        phase = self._value.phase(context.root_context)
        taxon_type = context.root_context.taxon_type

        if context.root_context.subrequest_only and phase > TelPhase.dimension:
            context.with_error(
                'Taxon used for subrequest cannot use complex calculation logic after merge() function.',
                location=self.location,
            )
        elif phase.is_metric() and TaxonTypeEnum.metric != taxon_type:
            context.with_error(
                f'Taxon is of type {taxon_type}, but calculation is for type {TaxonTypeEnum.metric}',
                location=self.location,
            )

        agg_type = self.aggregation_definition(context.root_context)
        if agg_type is None:
            context.with_error('It was not possible to deduce aggregation type', location=self.location)

        return context

    def return_type(self, context: TelRootContext) -> TelType:
        return self._value.return_type(context)

    def template_slugs(self, context: TelRootContext) -> Set[TaxonExpressionStr]:
        return self._value.template_slugs(context)

    def used_taxons(self, context: TelRootContext) -> UsedTaxonsContainer:
        return self._value.used_taxons(context)

    def return_data_sources(self, context: TelRootContext) -> Set[Optional[str]]:
        return self._value.return_data_sources(context)

    def invalid_value(self, context: TelRootContext) -> bool:
        return self._value.invalid_value(context)

    def __repr__(self):
        return f'Root({repr(self._value)})'

    @property
    def _graphviz_repr(self):
        return 'ROOT'

    @property
    def children(self) -> List[TelExpression]:
        return [self._value]

    def result(self, context: TelRootContext) -> TelQueryResult:
        if self._value.invalid_value(context):
            return TelQueryResult(null(), dialect=context.husky_dialect)
        else:
            return self._value.result(context)

    def phase(self, context: TelRootContext) -> TelPhase:
        return TelPhase.metric_post

    def rewrite(self, callback: Callable[[TelExpression], TelExpression], context: TelRootContext) -> TelExpression:
        return TelRoot.copy(self, callback(self._value.rewrite(callback, context)))

    def plan_phase_transitions(self, context: TelRootContext) -> TelExpression:
        if self._value.invalid_value(context):
            return self

        value = self._value.plan_phase_transitions(context)
        phase = value.phase(context)

        if context.subrequest_only:
            if TelPhase.dimension_data_source == phase:
                return TelRoot.copy(self, TelDimensionPhase.copy(self, value))
        elif phase.is_dimension():
            if TelPhase.dimension_data_source == phase:
                return TelRoot.copy(
                    self,
                    TelPostAggregationPhase.copy(
                        self,
                        TelAggregationPhase.copy(self, TelDimensionPhase.copy(self, value), label=context.taxon_slug),
                        aggregation=value.aggregation_definition(context),
                        label=context.taxon_slug,
                    ),
                )
            else:
                return TelRoot.copy(
                    self,
                    TelPostAggregationPhase.copy(
                        self,
                        TelAggregationPhase.copy(self, value, label=context.taxon_slug).plan_phase_transitions(context),
                        aggregation=value.aggregation_definition(context),
                        label=context.taxon_slug,
                    ),
                )
        elif TelPhase.any == phase:
            return TelRoot.copy(
                self,
                TelPostAggregationPhase.copy(
                    self,
                    TelAggregationPhase.copy(self, value).plan_phase_transitions(context),
                    aggregation=AggregationDefinition(type=AggregationType.not_set),
                ),
            )
        elif TelPhase.metric_pre == phase:
            aggregation = value.aggregation_definition(context)
            return TelRoot.copy(
                self, TelPostAggregationPhase.copy(self, value, aggregation=aggregation, label=context.taxon_slug)
            )

        return TelRoot.copy(self, value)

    def aggregation_definition(self, context: TelRootContext) -> Optional[AggregationDefinition]:
        """
        Calculates the aggregation definition for this node. None, if it isn't possible to deduce the definition
        """
        return self._value.aggregation_definition(context)

    def to_graphviz(self, context: TelRootContext) -> str:
        result = 'rankdir = TB;\n'
        for node, desc in self.graphviz_attributes(context).items():
            if desc:
                result += f'{node._graphviz_name()} [{desc}];\n'

        result += super().to_graphviz(context)

        result += 'subgraph legend {\n'
        result += '''
        aHtmlTable [
            shape=plaintext
            label=<

            <table border='0' cellborder='1' cellspacing='0' style='rounded'>
            <tr>
            <td><b>ID</b></td>
            <td><b>Node</b></td>
            <td><b>Phase</b></td>
            <td><b>Type</b></td>
            <td><b>Aggregation Type</b></td>
            </tr>
        '''
        result += _rows_to_graphviz(self.graphviz_description(context))
        result += '''
            </table>
        >];
        '''
        result += '}\n'

        return result

    def literal_value(self, context: TelRootContext) -> Optional[Any]:
        return self._value.literal_value(context)


def _rows_to_graphviz(rows: Dict[int, str]) -> str:
    result = ''
    for _, row in sorted(rows.items(), key=itemgetter(0)):
        result += f'{row}\n'

    return result
