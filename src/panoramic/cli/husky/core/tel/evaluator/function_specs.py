from abc import ABC, abstractmethod
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
)

from antlr4 import ParserRuleContext

from panoramic.cli.husky.core.taxonomy.aggregations import AggregationDefinition
from panoramic.cli.husky.core.tel.enums import TelDialectType
from panoramic.cli.husky.core.tel.evaluator.ast import TelExpression
from panoramic.cli.husky.core.tel.evaluator.context import (
    TelRootContext,
    TelValidationContext,
)
from panoramic.cli.husky.core.tel.result import UsedTaxonsContainer
from panoramic.cli.husky.core.tel.tel_phases import (
    ANY_PHASE,
    TelPhase,
    TelPhaseRange,
    TelPhaseSpec,
)
from panoramic.cli.husky.core.tel.tel_remote_interface import (
    TelArgumentExtractor,
    TelExpressionSpecGetter,
    TelRemoteSpecProtocol,
)
from panoramic.cli.husky.core.tel.types.tel_type_checks import TelTypeCheck
from panoramic.cli.husky.core.tel.types.tel_types import TelReturnTypeSpec, TelType
from panoramic.cli.husky.service.utils.taxon_slug_expression import TaxonExpressionStr

T = TypeVar('T')


class AcceptedArgSpecKind(Enum):
    TYPE = "type"
    ANY = "any"
    VAR_ARGS = "varargs"
    COMPATIBLE = "compatible"


class AcceptedArg(TelRemoteSpecProtocol, ABC, Generic[T]):
    """
    Description of an argument to be accepted by a function. If the provided argument does not match the description,
    it is rejected with a validation error.
    """

    _name: str
    """
    Name of the argument
    """
    _optional: bool
    """
    Flag whether the argument is required or optional
    """
    _must_be_constant: bool
    """
    Flag whether the argument must be a constant (no taxons used)
    """
    _phase_range: TelPhaseRange
    """
    Accepted phases
    """

    @abstractmethod
    def validate(
        self, position: int, fn_name: str, expr: Optional[T], location: ParserRuleContext, context: TelValidationContext
    ) -> TelValidationContext:
        pass

    @abstractmethod
    def extract(self, position: int, args: List[TelExpression]) -> Tuple[Optional[T], int]:
        """
        Extract argument from the list of arguments.
        :param position: position of the argument to extract
        :param args: function arguments
        :return: tuple of extracted argument(s) and a number of extracted arguments
        """
        pass

    @property
    def optional(self):
        return self._optional

    @property
    def name(self):
        return self._name

    def _validate_single_arg(
        self,
        position: int,
        fn_name: str,
        expr: Optional[TelExpression],
        context: TelValidationContext,
        type_check: TelTypeCheck,
        must_be_constant=False,
    ) -> TelValidationContext:
        """
        Validate the argument against the provided requirements
        :param position: position of the argument
        :param fn_name: name of the function
        :param expr: node with containing the argument in the given position
        :param context: validation context
        :param type_check: type check
        :param must_be_constant: whether the argument must be a constant (no taxons used)
        :return: validation context
        """
        if self._optional and not expr:
            return context
        elif expr:
            return_type = expr.return_type(context.root_context)
            type_matches = type_check(return_type)

            if must_be_constant and not return_type.is_constant:
                context.with_error(
                    f'Argument {position + 1} in function {fn_name} must be a constant of type: {str(type_check)}',
                    location=expr.location,
                )
            elif not type_matches:
                context.with_error(
                    f'Argument {position + 1} in function {fn_name} must be of type: {str(type_check)}',
                    location=expr.location,
                )

        return self._validate_phase(position, fn_name, expr, context)

    def _validate_phase(
        self, position: int, fn_name: str, expr: Optional[TelExpression], context: TelValidationContext
    ):
        if expr and expr.phase(context.root_context) not in self._phase_range:
            context.with_error(
                f'Argument {position + 1} in function {fn_name} must have a phase {self._phase_range}',
                location=expr.location,
            )

        return context


class TypeAcceptedArg(AcceptedArg[TelExpression]):
    """
    Argument required to be of a specified type.
    """

    def __init__(
        self, name: str, type_check: TelTypeCheck, must_be_constant=False, optional=False, phase_range=ANY_PHASE
    ):
        self._name = name
        self._type_check = type_check
        self._optional = optional
        self._must_be_constant = must_be_constant
        self._phase_range = phase_range

    def validate(
        self,
        position: int,
        fn_name: str,
        expr: Optional[TelExpression],
        location: ParserRuleContext,
        context: TelValidationContext,
    ) -> TelValidationContext:
        return self._validate_single_arg(
            position, fn_name, expr, context, self._type_check, must_be_constant=self._must_be_constant
        )

    def extract(self, position: int, args: List[TelExpression]) -> Tuple[Optional[TelExpression], int]:
        try:
            return args[position], 1
        except IndexError:
            return None, 0

    def to_remote_spec(self) -> Dict[str, Any]:
        return {
            "kind": AcceptedArgSpecKind.TYPE,
            "dataTypes": [dt.value for dt in self._type_check.data_types],
            "optional": self._optional,
            "mustBeConstant": self._must_be_constant,
            "phaseRange": self._phase_range.to_remote_spec(),
        }


class AnyTypeAcceptedArg(AcceptedArg[TelExpression]):
    """
    Argument required to be of any type.
    """

    def __init__(self, name: str, optional=False, phase_range=ANY_PHASE):
        self._name = name
        self._optional = optional
        self._phase_range = phase_range

    def validate(
        self,
        position: int,
        fn_name: str,
        expr: Optional[TelExpression],
        location: ParserRuleContext,
        context: TelValidationContext,
    ) -> TelValidationContext:
        if self._optional and not expr:
            return context
        elif not self._optional and not expr:
            context.with_error(
                f'Missing required argument in position {position} of function {fn_name}', location=location
            )

        return self._validate_phase(position, fn_name, expr, context)

    def extract(self, position: int, args: List[TelExpression]) -> Tuple[Optional[TelExpression], int]:
        try:
            return args[position], 1
        except IndexError:
            return None, 0

    def to_remote_spec(self) -> Dict[str, Any]:
        return {
            "kind": AcceptedArgSpecKind.ANY,
            "optional": self._optional,
            "phaseRange": self._phase_range.to_remote_spec(),
        }


class VariableLengthAcceptedArg(AcceptedArg[Iterable[TelExpression]]):
    """
    Variable length arguments, of a specified type.
    """

    def __init__(
        self, name: str, type_check: TelTypeCheck, must_be_constant=False, optional=False, phase_range=ANY_PHASE
    ):
        self._name = name
        self._type_check = type_check
        self._optional = optional
        self._must_be_constant = must_be_constant
        self._phase_range = phase_range

    def validate(
        self,
        position: int,
        fn_name: str,
        exprs: Optional[Iterable[TelExpression]],
        location: ParserRuleContext,
        context: TelValidationContext,
    ) -> TelValidationContext:
        if not self._optional and not exprs:
            context.with_error(f'{fn_name} requires at least 1 argument', location=location)
        elif exprs:
            for idx, expr in enumerate(exprs):
                self._validate_single_arg(
                    position + idx, fn_name, expr, context, self._type_check, must_be_constant=self._must_be_constant
                )

        return context

    def extract(self, position: int, args: List[TelExpression]) -> Tuple[Optional[Iterable[TelExpression]], int]:
        try:
            res = args[position:]
            return res, len(res)
        except IndexError:
            return None, 0

    def to_remote_spec(self) -> Dict[str, Any]:
        return {
            "kind": AcceptedArgSpecKind.VAR_ARGS,
            "dataTypes": [dt.value for dt in self._type_check.data_types],
            "optional": self._optional,
            "mustBeConstant": self._must_be_constant,
            "phaseRange": self._phase_range.to_remote_spec(),
        }


class CompatibleTypesVariableLengthAcceptedArg(AcceptedArg[Iterable[TelExpression]]):
    """
    Variable length arguments, that must have compatible types, see `TelType.are_compatible_data_types`.
    """

    def __init__(self, name: str, optional=False, phase_range=ANY_PHASE):
        self._name = name
        self._optional = optional
        self._phase_range = phase_range

    def validate(
        self,
        position: int,
        fn_name: str,
        exprs: Optional[Iterable[TelExpression]],
        location: ParserRuleContext,
        context: TelValidationContext,
    ) -> TelValidationContext:
        if self._optional and not exprs:
            return context
        elif not self._optional and not exprs:
            return context.with_error(f'{fn_name} requires at least 1 argument', location=location)
        elif exprs and not TelType.are_compatible_data_types(
            [expr.return_type(context.root_context) for expr in exprs]
        ):
            context.with_error(f'Arguments in function {fn_name} must have compatible data types', location=location)

            for expr in exprs:
                self._validate_phase(position, fn_name, expr, context)

        return context

    def extract(self, position: int, args: List[TelExpression]) -> Tuple[Optional[Iterable[TelExpression]], int]:
        try:
            res = args[position:]
            return res, len(res)
        except IndexError:
            return None, 0

    def to_remote_spec(self) -> Dict[str, Any]:
        return {
            "kind": AcceptedArgSpecKind.COMPATIBLE,
            "optional": self._optional,
            "phaseRange": self._phase_range.to_remote_spec(),
        }


class InvalidValueSpecKind(Enum):
    ALL = "all"  # when all arguments are invalid
    SOME = "some"  # when some arguments are invalid, argumentExtractor must be provided
    NONE = "none"  # when no arguments are provided


class TelInvalidValueSpec(TelRemoteSpecProtocol, TelExpressionSpecGetter[bool], ABC):
    pass


class TelAllArgsInvalidValueSpec(TelInvalidValueSpec):
    def get(self, args, context) -> bool:
        return all([arg.invalid_value(context) for arg in args])

    def to_remote_spec(self) -> Dict[str, Any]:
        return {"kind": InvalidValueSpecKind.ALL}


class TelSomeArgsInvalidValueSpec(TelInvalidValueSpec):
    def __init__(self, argument_extractor: TelArgumentExtractor):
        self._argument_extractor = argument_extractor

    def get(self, args, context) -> bool:
        return all([arg.invalid_value(context) for arg in self._argument_extractor.extract_arguments(args)])

    def to_remote_spec(self) -> Dict[str, Any]:
        return {"kind": InvalidValueSpecKind.SOME, "argumentExtractor": self._argument_extractor.to_remote_spec()}


class TelNoArgsInvalidValueSpec(TelInvalidValueSpec):
    def get(self, args, context) -> bool:
        return not args

    def to_remote_spec(self) -> Dict[str, Any]:
        return {"kind": InvalidValueSpecKind.NONE}


class TelFunction(TelExpression, ABC):
    """
    Base class for Tel functions. Implementing functions extend as needed, but by default everything depends on
    arguments, unless different behaviour is implemented by the subclass.
    """

    _name: str
    _args: List[TelExpression]
    _drop_invalid_args: bool = False
    """
    Flag whether the function should ignore invalid args upon construction.
    """
    _supported_dialects: Set[TelDialectType] = {TelDialectType.TAXON, TelDialectType.MODEL}

    def __init__(self, context: TelRootContext, args: List[TelExpression]):
        super().__init__(context)

        if self._drop_invalid_args:
            self._args = [arg for arg in args if not arg.invalid_value(context)]
        else:
            self._args = args

    def validate(self, context: TelValidationContext) -> TelValidationContext:
        dialect_type: TelDialectType = context.root_context.tel_dialect.type
        if dialect_type not in self._supported_dialects:
            return context.with_error(
                f'Dialect {dialect_type.name} is not supported by function {self._name}', self.location
            )

        for arg in self._args:
            arg.validate(context)

        return context

    def phase(self, context: TelRootContext) -> TelPhase:
        return TelPhase.max([arg.phase(context) for arg in self._args])

    def template_slugs(self, context: TelRootContext) -> Set[TaxonExpressionStr]:
        template_slugs = set()

        for arg in self._args:
            template_slugs.update(arg.template_slugs(context))

        return template_slugs

    def return_data_sources(self, context: TelRootContext) -> Set[Optional[str]]:
        return_data_sources = set()

        for arg in self._args:
            return_data_sources.update(arg.return_data_sources(context))

        return return_data_sources

    def used_taxons(self, context: TelRootContext) -> UsedTaxonsContainer:
        used_taxons = UsedTaxonsContainer()

        for arg in self._args:
            used_taxons.update_from(arg.used_taxons(context))

        return used_taxons

    def rewrite(self, callback: Callable[[TelExpression], TelExpression], context: TelRootContext) -> TelExpression:
        # Just optimize the arguments
        return self.__class__.copy(self, [callback(arg.rewrite(callback, context)) for arg in self._args])

    def invalid_value(self, context: TelRootContext) -> bool:
        return any([arg.invalid_value(context) for arg in self._args])

    @property
    def children(self) -> List[TelExpression]:
        return self._args

    def __repr__(self):
        return f'{self._name}({", ".join(map(repr, self._args))})'

    @property
    def _graphviz_repr(self):
        return f'{self._name}({", ".join(map(lambda arg: arg._graphviz_repr, self._args))})'

    def plan_phase_transitions(self, context: TelRootContext) -> TelExpression:
        # Just plan phase transitions of the arguments
        return self.__class__.copy(self, [arg.plan_phase_transitions(context) for arg in self._args])

    def return_type(self, context: TelRootContext) -> TelType:
        return TelType.return_common_type([arg.return_type(context) for arg in self._args]).copy(is_constant=False)

    def aggregation_definition(self, context: TelRootContext) -> Optional[AggregationDefinition]:
        """
        Calculates the aggregation definition for this node. None, if it isn't possible to deduce the definition
        """
        agg_definitions = [arg.aggregation_definition(context) for arg in self._args]
        return AggregationDefinition.common_defined_definition(agg_definitions)

    def literal_value(self, context: TelRootContext) -> Optional[Any]:
        return None


class TelTypedFunction(TelFunction, ABC):
    """
    Tel Function with types of required arguments defined via `expected_arg_types` method.
    """

    expected_arg_types: Iterable[AcceptedArg]
    phase_spec: Optional[TelPhaseSpec] = None
    return_type_spec: Optional[TelReturnTypeSpec] = None
    invalid_value_spec: Optional[TelInvalidValueSpec] = None

    def validate(self, context: TelValidationContext) -> TelValidationContext:
        validate_expected_args(self._name, list(self.expected_arg_types), self._args, self.location, context)
        return super().validate(context)

    def phase(self, context: TelRootContext) -> TelPhase:
        if self.phase_spec:
            return self.phase_spec.get(self._args, context)
        else:
            return super().phase(context)

    def return_type(self, context: TelRootContext) -> TelType:
        if self.return_type_spec:
            return self.return_type_spec.get(self._args, context)
        else:
            return super().return_type(context)

    def invalid_value(self, context: TelRootContext) -> bool:
        if self.invalid_value_spec:
            return self.invalid_value_spec.get(self._args, context)
        else:
            return super().invalid_value(context)


def validate_expected_args(
    name: str,
    expected_args: List[AcceptedArg],
    args: List[TelExpression],
    location: ParserRuleContext,
    context: TelValidationContext,
) -> TelValidationContext:
    required_arg_names = [arg.name for arg in expected_args if not arg.optional]
    optional_arg_names = [arg.name for arg in expected_args if arg.optional]

    if len(args) < len(required_arg_names):
        error = f'{name} requires {len(required_arg_names)}'
        if optional_arg_names:
            error += f' or {len(expected_args)}'

        error += f' arguments: {", ".join(required_arg_names)}'

        if optional_arg_names:
            error += f'(optionally also, {", ".join(optional_arg_names)})'

        error += ', but'
        if not args:
            error += ' none were'
        elif len(args) == 1:
            error += ' only one was'
        else:
            error += f' {len(args)} were'

        error += ' given'

        return context.with_error(error, location=location)

    extracted_args = 0
    for pos, expected_arg_type in enumerate(expected_args):
        arg, cnt = expected_arg_type.extract(pos, args)
        extracted_args += cnt
        if not arg and not expected_arg_type.optional:
            context.with_error(f'Missing required argument {pos} in function {name}', location)
            continue
        else:
            expected_arg_type.validate(pos, name, arg, location, context)

    if extracted_args != len(args):
        context.with_error(
            f'Function {name} was provided with an incorrect number of arguments {len(args)}, '
            f'instead of expected {extracted_args}',
            location=location,
        )

    return context
