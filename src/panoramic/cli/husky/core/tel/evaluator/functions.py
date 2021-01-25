from abc import ABC, abstractmethod
from collections import Counter
from typing import Callable, Iterable, List, Mapping, Optional, Set, Type, cast

import pytz
import sqlalchemy
from sqlalchemy import (
    DECIMAL,
    Boolean,
    Integer,
    Numeric,
    String,
    case,
    false,
    func,
    literal,
    literal_column,
    null,
    or_,
    text,
)
from sqlalchemy.sql import ClauseElement, functions

from panoramic.cli.husky.common.sqlalchemy_ext import (
    ConvertTimezone,
    DateTrunc,
    Extract,
    ParseDate,
    SplitPart,
    TimestampDiff,
)
from panoramic.cli.husky.core.sql_alchemy_util import (
    LIKE_PATTERN_ESCAPE_CHAR,
    compile_query,
    escape_special_character_in_like_pattern,
    safe_quote_identifier,
)
from panoramic.cli.husky.core.taxonomy.aggregations import AggregationDefinition
from panoramic.cli.husky.core.taxonomy.enums import AggregationType
from panoramic.cli.husky.core.taxonomy.override_mapping.types import (
    OverrideMappingTelInformation,
)
from panoramic.cli.husky.core.tel.enums import TelDialectType
from panoramic.cli.husky.core.tel.evaluator.ast import (
    TelAggregationPhase,
    TelDimensionPhase,
    TelExpression,
    TelPostAggregationPhase,
    TelRoot,
)
from panoramic.cli.husky.core.tel.evaluator.context import (
    TelRootContext,
    TelValidationContext,
)
from panoramic.cli.husky.core.tel.evaluator.expressions import (
    TelBoolean,
    TelDivision,
    TelInteger,
    TelLogicalOperation,
    TelString,
)
from panoramic.cli.husky.core.tel.evaluator.function_specs import (
    AcceptedArg,
    AnyTypeAcceptedArg,
    CompatibleTypesVariableLengthAcceptedArg,
    TelFunction,
    TelNoArgsInvalidValueSpec,
    TelSomeArgsInvalidValueSpec,
    TelTypedFunction,
    TypeAcceptedArg,
    VariableLengthAcceptedArg,
    validate_expected_args,
)
from panoramic.cli.husky.core.tel.evaluator.result import (
    TelQueryResult,
    result_with_template,
)
from panoramic.cli.husky.core.tel.result import PostFormula
from panoramic.cli.husky.core.tel.tel_phases import (
    TelFixedPhaseSpec,
    TelMaximumPhaseSpec,
    TelMinimumPhaseSpec,
    TelPhase,
    TelPhaseRange,
)
from panoramic.cli.husky.core.tel.tel_remote_interface import (
    EXTRACT_FIRST_ARGUMENT,
    TelSlicingArgumentExtractor,
)
from panoramic.cli.husky.core.tel.types.tel_type_checks import (
    CheckAnyType,
    CheckBooleanType,
    CheckDatetimeType,
    CheckIntegerType,
    CheckNumberOrStringOrBooleanType,
    CheckNumberOrStringOrDateType,
    CheckNumberType,
    CheckStringType,
)
from panoramic.cli.husky.core.tel.types.tel_types import (
    TelCopyReturnTypeSpec,
    TelDataType,
    TelFixedReturnTypeSpec,
    TelType,
)
from panoramic.cli.husky.service.blending.features.override_mapping.sql import (
    OverrideMappingSql,
)


class TelBaseCondition(TelFunction, ABC):
    """
    Base class for if-like functions.
    """

    _max_conditions: int = 1
    """
    Maximum number of conditions accepted by the implementing function.
    """

    @property
    def _conditions(self) -> List[TelExpression]:
        if self._negative:
            return self._args[:-1:2]  # every other arg, except the last
        else:
            return self._args[::2]  # every other arg

    @property
    def _positives(self) -> List[TelExpression]:
        if self._negative:
            return self._args[1:-1:2]  # every other arg, starting with the second, except the last
        else:
            return self._args[1::2]  # every other arg, starting with the second

    @property
    def _negative(self) -> Optional[TelExpression]:
        if len(self._args) > 2 and len(self._args) % 2:
            return self._args[-1]
        else:
            return None

    def invalid_value(self, context: TelRootContext) -> bool:
        if self._negative and not self._negative.invalid_value(context):
            return False

        for condition, outcome in zip(self._conditions, self._positives):
            if not condition.invalid_value(context) and not outcome.invalid_value(context):
                return False

        return True

    def validate(self, context: TelValidationContext) -> TelValidationContext:
        condition_phases = [
            arg.phase(context.root_context) for arg in self._conditions if not arg.invalid_value(context.root_context)
        ]
        positive_phases = [
            arg.phase(context.root_context) for arg in self._positives if not arg.invalid_value(context.root_context)
        ]
        outcome_phases = positive_phases
        if (self._negative is not None) and (not self._negative.invalid_value(context.root_context)):
            outcome_phases.append(self._negative.phase(context.root_context))

        if any(phase.is_dimension() for phase in outcome_phases) and any(phase.is_metric() for phase in outcome_phases):
            context.with_error(f'{self._name} cannot combine dimension and metric outcomes', location=self.location)

        if (
            any(phase.is_metric() for phase in condition_phases)
            and len(outcome_phases) > 0
            and max(outcome_phases).is_dimension()
        ):
            context.with_error(
                f'Condition arguments in function {self._name} must be dimension taxons when the outcome is dimension',
                location=self.location,
            )

        if (
            any(phase.is_dimension() for phase in condition_phases)
            and len(outcome_phases) > 0
            and max(outcome_phases) is TelPhase.metric_post
        ):
            context.with_error(
                f'Condition arguments in function {self._name} must be metric taxons when the outcome is post-aggregation metric',
                location=self.location,
            )

        if TelDataType.UNKNOWN is self.return_type(context.root_context).data_type:
            context.with_error(
                f'Outcome arguments in function {self._name} must have compatible data types', location=self.location
            )

        return super().validate(context)

    def return_type(self, context: TelRootContext) -> TelType:
        return TelType.return_common_type(
            [arg.return_type(context) for arg in self._positives + ([self._negative] if self._negative else [])]
        ).copy(is_constant=False)

    def result(self, context: TelRootContext) -> TelQueryResult:
        result_phase = self.phase(context)

        condition_results = [(arg.result(context), arg.invalid_value(context)) for arg in self._conditions]
        positive_results = [(arg.result(context), arg.invalid_value(context)) for arg in self._positives]

        sql_when_pairs = [
            (
                cond.sql if not cond_invalid else false(),
                pos.sql if not pos_invalid else literal(0) if result_phase.is_metric() else null(),
            )
            for (cond, cond_invalid), (pos, pos_invalid) in zip(condition_results, positive_results)
        ]

        template_when_pairs = [
            (
                cond.template_or_sql if not cond_invalid else false(),
                pos.template_or_sql
                if not pos_invalid
                else literal(0, Integer())
                if result_phase.is_metric()
                else null(),
            )
            for (cond, cond_invalid), (pos, pos_invalid) in zip(condition_results, positive_results)
        ]

        negative_invalid = self._negative.invalid_value(context) if self._negative else False
        evaluated_negative_result = self._negative.result(context) if self._negative else None

        negative_sql = (
            evaluated_negative_result.sql
            if evaluated_negative_result and not negative_invalid
            else literal(0)
            if result_phase.is_metric()
            else null()
        )

        negative_template = (
            evaluated_negative_result.template_or_sql
            if evaluated_negative_result and not negative_invalid
            else literal(0)
            if result_phase.is_metric()
            else null()
        )

        sql = case(sql_when_pairs, else_=negative_sql)
        template = case(template_when_pairs, else_=negative_template)

        return TelQueryResult.merge(
            sql,
            context.husky_dialect,
            *[res for res, invalid in condition_results if not invalid],
            *[res for res, invalid in positive_results if not invalid],
            evaluated_negative_result if not negative_invalid else None,
            template=template,
        )

    def plan_phase_transitions(self, context: TelRootContext) -> TelExpression:
        result_phase = self.phase(context)
        # Move all arguments to the post-aggregation phase
        return self.__class__.copy(
            self,
            [
                self._plan_for_phase(
                    result_phase, arg, context, arg.aggregation_definition(context)
                ).plan_phase_transitions(context)
                for arg in self._args
            ],
        )

    @staticmethod
    def _plan_for_phase(
        phase: TelPhase,
        val: TelExpression,
        context: TelRootContext,
        aggregation_definition: Optional[AggregationDefinition],
    ) -> 'TelExpression':

        if TelPhase.dimension is phase:
            return TelDimensionPhase.copy(val, val.plan_phase_transitions(context))
        elif TelPhase.metric_pre is phase:
            return TelAggregationPhase.copy(val, val.plan_phase_transitions(context), aggregation_definition)
        elif TelPhase.metric_post is phase:
            return TelPostAggregationPhase.copy(val, val.plan_phase_transitions(context), aggregation_definition)

        return val


class TelIff(TelTypedFunction, TelBaseCondition):
    """
    if-then-else condition

    `iff` is a function of two or three arguments, depending on whether there is an `else` part or not

    > Example with only true outcome

    ```
    iff(twitter|spend > 100, impressions + twitter|spend - 2)
    ```

    > Example with both outcomes

    ```
    iff(twitter|spend > 100, impressions / 100, impressions * 2)
    ```

    :param boolean condition: conditional expression, in the same phase as the outcome arguments (dimension or metric)
    :param any positive_outcome: any expression in the same phase as the condition, returned when the condition evaluated to true
    :param any negative_outcome: (optional) any expression in the same phase as the condition, returned when the condition evaluated to false

    :returns any: either result of the `positive_outcome` or `negative_outcome` expression, depending on the result of the `condition`

    :raises ValidationError: (number of arguments) != 2 or 3
    :raises ValidationError: positive and negative outcome have different result phases
    :raises ValidationError: the outcomes have incompatible return types
    """

    """
    iff(condition: BOOLEAN, positive_outcome: T(ANY), ?negative_outcome: T): T
    """

    _name = 'iff'
    expected_arg_types = (
        TypeAcceptedArg('condition', CheckBooleanType()),
        AnyTypeAcceptedArg('positive_outcome'),
        AnyTypeAcceptedArg('negative_outcome', optional=True),
    )

    def aggregation_definition(self, context: TelRootContext) -> Optional[AggregationDefinition]:
        """
        Calculates the aggregation definition for this node. None, if it isn't possible to deduce the definition
        """
        agg_definitions = [arg.aggregation_definition(context) for arg in self.children[1:]]

        return AggregationDefinition.common_defined_definition(agg_definitions, True, context.taxon_type)


class TelIfs(TelBaseCondition):
    """
    switch expression

    `ifs` is a function of up to 100 pairs of a condition and an outcome expression with one additional, optional `else`
    expresion that is returned, if none of the conditions matched

    > Example with two conditions and no `else` expression

    ```
    ifs(fb_tw_merged_ad_id == 'tw', twitter|ad_id, fb_tw_merged_ad_id == 'fb', facebook_ads|ad_id)
    ```

    > Example with two conditions and an `else` expression

    ```
    ifs(fb_tw_merged_ad_id == 'tw', twitter|ad_id, fb_tw_merged_ad_id == 'fb', facebook_ads|ad_id, "unknown")
    ```

    :param boolean condition: (many) conditional expressions, in the same phase as the outcome arguments (dimension or metric)
    :param any positive_outcome: (many) any expressions in the same phase as the condition, returned when the condition evaluated to true
    :param any negative_outcom: (optional) any expression in the same phase as the condition, returned when all the conditions evaluated to false

    :returns any: the first result of the `positive_outcome` of the pair where the `condition` matched, or else the `negative_outcome` expression, if specified

    :raises ValidationError: there were less than 2 arguments or more than 100 pairs of condition and outcome
    :raises ValidationError: there are outcomes in both dimension and metric outcomes
    :raises ValidationError: the outcomes have incompatible return types
    """

    """
    ifs(*[condition: BOOLEAN, expression_true: T(ANY)], ?expression_false: T): T
    """

    _name = 'ifs'
    _max_conditions = 100

    def validate(self, context: TelValidationContext) -> TelValidationContext:
        expected_args: List[AcceptedArg] = []

        for idx in range(len(self._conditions)):
            # condition
            expected_args.append(TypeAcceptedArg(f'condition_{idx + 1}', CheckBooleanType()))
            # positive outcome
            expected_args.append(AnyTypeAcceptedArg(f'positive_outcome_{idx + 1}'))

        # negative outcome
        expected_args.append(AnyTypeAcceptedArg('negative_outcome', optional=True))

        validate_expected_args(self._name, expected_args, self._args, self.location, context)

        return super().validate(context)

    def aggregation_definition(self, context: TelRootContext) -> Optional[AggregationDefinition]:
        """
        Calculates the aggregation definition for this node. None, if it isn't possible to deduce the definition
        """
        agg_definitions = [arg.aggregation_definition(context) for arg in self.children[1::2]]

        return AggregationDefinition.common_defined_definition(agg_definitions, True, context.taxon_type)


class TelCoalesce(TelTypedFunction):
    """
    return the first non-null value in a list

    `coalesce` function accepts any number of arguments and returns the first valid, non-null value

    > Example

    ```
    coalesce(?facebook_ads|spend, twitter|spend)
    ```

    :param any expression: (many) any expression

    :returns any: the first valid, non-null, argument

    :raises ValidationError: `(number of arguments) == 0`
    :raises ValidationError: arguments are not of compatible types
    """

    """
    coalesce(*expression: T(ANY)): T
    """

    _name = 'coalesce'
    _drop_invalid_args = True
    expected_arg_types = (CompatibleTypesVariableLengthAcceptedArg('expression'),)
    phase_spec = TelFixedPhaseSpec(TelPhase.metric_post)
    invalid_value_spec = TelNoArgsInvalidValueSpec()

    def result(self, context: TelRootContext) -> TelQueryResult:
        results = [arg.result(context) for arg in self._args]
        sql = functions.coalesce(*[result.sql for result in results])
        template = functions.coalesce(*[result.template_or_sql for result in results])
        return TelQueryResult.merge(sql, context.husky_dialect, *results, template=template)

    def plan_phase_transitions(self, context: TelRootContext) -> TelExpression:
        # Move aggregation phase arguments to the post aggregation phase, keep dimension arguments as is
        return TelCoalesce.copy(
            self,
            [
                TelPostAggregationPhase.copy(
                    self, arg.plan_phase_transitions(context), self.aggregation_definition(context)
                )
                if arg.phase(context) == TelPhase.metric_pre
                else arg.plan_phase_transitions(context)
                for arg in self._args
            ],
        )

    def rewrite(self, callback: Callable[[TelExpression], TelExpression], context: TelRootContext) -> TelExpression:
        #  Coalesce requires at least two args. No reason to coalesce with just one arg.
        if len(self._args) == 1:
            return self._args[0].rewrite(callback, context)
        else:
            return TelCoalesce.copy(self, [callback(arg.rewrite(callback, context)) for arg in self._args])


class TelConcat(TelTypedFunction):
    """
    concatenation, or joining, of two or more string values in an end-to-end manner

    `concat` function returns a concatenation of its arguments, without any separator. Arguments must be from the same data source

    > Example

    ```
    concat(?twitter|ad_id, ?twitter|ad_name)
    ```

    :param any expression: (many) expression to concatenate on the output

    :returns string: concatenation of the provided strings

    :raises ValidationError: taxon fields are from different data sources or before `merge()` was applied
    """

    """
    concat(*expression: ANY): STRING
    """

    _name = 'concat'
    expected_arg_types = (VariableLengthAcceptedArg('expression', CheckAnyType(), optional=True),)
    return_type_spec = TelCopyReturnTypeSpec(
        data_type=TelDataType.STRING, argument_extractor=TelSlicingArgumentExtractor(0), is_constant=False
    )

    def validate(self, context: TelValidationContext) -> TelValidationContext:
        if len(self.return_data_sources(context.root_context)) > 1:
            context.with_error(
                'concat accepts only taxons from same data source or after merge() is applied', location=self.location
            )

        return super().validate(context)

    def invalid_value(self, context: TelRootContext) -> bool:
        return self._args == 0 or any([arg.invalid_value(context) for arg in self._args])

    def result(self, context: TelRootContext) -> TelQueryResult:
        results = [arg.result(context) for arg in self._args]
        sql = functions.concat(*[result.sql for result in results])
        template = functions.concat(*[result.template_or_sql for result in results])
        return TelQueryResult.merge(sql, context.husky_dialect, *results, template=template)


class TelMerge(TelTypedFunction):
    """
    return the first valid, non-null, value across multiple data sources

    `merge` is the only function that can combine dimensions across data sources. It allows only one taxon per data source.

    > Example:

    ```
    merge(?twitter|ad_id, ?facebook_ads|ad_id)
    ```

    :param any expression: (many) expression, potentially, from different data-sources, from which the function will pick the first non-null, valid value as the result

    :returns any: the first valid, non-null, value across multiple data sources

    :raises ValidationError: `(number of arguments) < 1`
    :raises ValidationError: arguments are not of compatible types
    :raises ValidationError: there are more than one taxon fields per data source
    """

    """
    merge(*expression: T(ANY)): T
    """

    _name = 'merge'
    _drop_invalid_args = True
    _single_data_source: bool
    _supported_dialects = {TelDialectType.TAXON}

    expected_arg_types = (CompatibleTypesVariableLengthAcceptedArg('expression', optional=True),)
    phase_spec = TelFixedPhaseSpec(TelPhase.dimension)
    invalid_value_spec = TelNoArgsInvalidValueSpec()

    def __init__(self, context: TelRootContext, args: List[TelExpression]):
        super().__init__(context, args)
        self._single_data_source = bool(context.allowed_data_sources and len(context.allowed_data_sources) == 1)

    def validate(self, context: TelValidationContext) -> TelValidationContext:
        if self._args:
            child_data_sources: List[Optional[str]] = []
            for arg in self._args:
                child_data_sources.extend(arg.return_data_sources(context.root_context))

            # Check that the arguments are correct (one taxon per ds, no None namespace taxons)
            child_data_sources_set = set(child_data_sources)
            if len(child_data_sources_set) != len(child_data_sources):
                duplicate_ds_str = ', '.join(
                    # 'global' data source in case of global taxon (None in python)
                    [ds or 'global' for ds, count in Counter(child_data_sources).items() if count > 1]
                )
                context.with_error(
                    f'merge() accepts only one taxon per distinct data source, but more taxons were provided for following data sources: {duplicate_ds_str}',
                    location=self.location,
                )

        return super().validate(context)

    def return_data_sources(self, context: TelRootContext) -> Set[Optional[str]]:
        return {None}

    def plan_phase_transitions(self, context: TelRootContext) -> TelExpression:
        # Move the argument to the dimension phase
        result = TelMerge.copy(
            self, [TelDimensionPhase.copy(self, arg.plan_phase_transitions(context)) for arg in self._args]
        )
        cast(TelMerge, result)._single_data_source = self._single_data_source
        return result

    def result(self, context: TelRootContext) -> TelQueryResult:
        results = [arg.result(context) for arg in self._args]

        if len(results) == 1:
            sql = results[0].sql
            template = results[0].template_or_sql
        else:
            sql = functions.coalesce(*[result.sql for result in results])
            template = functions.coalesce(*[result.template_or_sql for result in results])

        return TelQueryResult.merge(sql, context.husky_dialect, *results, template=template)

    def rewrite(self, callback: Callable[[TelExpression], TelExpression], context: TelRootContext) -> TelExpression:
        if self._single_data_source:
            return self._args[0].rewrite(callback, context)
        else:
            result = TelMerge.copy(self, [callback(arg.rewrite(callback, context)) for arg in self._args])
            cast(TelMerge, result)._single_data_source = self._single_data_source
            return result


ALLOWED_TIMEZONES = {tz_name for tz_name in pytz.all_timezones}
"""
Set of allowed timezones.
Most pytz tz names should match SF tz https://docs.snowflake.com/en/sql-reference/functions/convert_timezone.html
"""


class TelConvertTimeZone(TelTypedFunction):
    """
    converts a timestamp to another timezone

    `convert_timezone` converts a timestamp expression from source to destination timezone. For timestamps with timezone
    just provide destination timezone. For timestamps without timezone you have to provide both source and destination timezones.

    > Example for timestamp with timezone

    ```
    convert_timezone(timestamp_tz, "Europe/Prague")
    ```

    > Example for timestamp without timezone

    ```
    convert_timezone(timestamp_ntz, "America/Los_Angeles", "Europe/Prague")
    ```

    :param datetime expression: taxon datetime field to be converted
    :param string timezone_from: (optional) source timezone to convert from, supported time zones are defined by IANA database
    :param string timezone_to: destination timezone to convert to, supported time zones are defined by IANA database

    :returns datetime: datetime in the specified destination timezone

    :raises ValidationError: `(number of arguments) != 2 or 3`
    :raises ValidationError: `expression` is in an aggregation phase
    :raises ValidationError: `timezone_from` is not a valid IANA timezone name
    :raises ValidationError: `timezone_to` is not a valid IANA timezone name
    """

    """
    convert_timezone(expression: DATETIME, timezone_to: STRING): DATETIME
    convert_timezone(expression: DATETIME, timezone_from: STRING, timezone_to: STRING): DATETIME
    """

    _name = 'convert_timezone'
    expected_arg_types = (
        TypeAcceptedArg('expression', CheckDatetimeType(), phase_range=TelPhaseRange(upper_limit=TelPhase.dimension)),
        TypeAcceptedArg('timezone_from', CheckStringType(), must_be_constant=True),
        TypeAcceptedArg('timezone_to', CheckStringType(), must_be_constant=True, optional=True),
    )
    phase_spec = TelMaximumPhaseSpec(TelPhase.dimension_data_source, EXTRACT_FIRST_ARGUMENT)
    return_type_spec = TelCopyReturnTypeSpec(argument_extractor=EXTRACT_FIRST_ARGUMENT, is_constant=False)

    @property
    def _datetime(self) -> TelExpression:
        return self._args[0]

    @property
    def _timezones(self) -> List[TelExpression]:
        return self._args[1:]

    def _timezones_parsed(self, context: TelRootContext):
        return [timezone.literal_value(context) for timezone in self._timezones]

    def validate(self, context: TelValidationContext) -> TelValidationContext:
        for idx in range(len(self._timezones)):
            if self._timezones_parsed(context.root_context)[idx] not in ALLOWED_TIMEZONES:
                context.with_error(
                    f'Argument {idx + 2} in function convert_timezone is not a valid timezone name',
                    location=self._timezones[idx].location,
                )

        return super().validate(context)

    def result(self, context: TelRootContext) -> TelQueryResult:
        dt_result = self._datetime.result(context)
        from_tz_result = self._timezones[0].result(context)

        def sql(dt, from_tz, to_tz):
            return ConvertTimezone(dt, from_tz, to_tz)

        sql, template = result_with_template(
            sql,
            dt=dt_result,
            from_tz=from_tz_result,
            to_tz=self._timezones[1].result(context) if len(self._timezones) == 2 else None,
        )
        return dt_result.update(sql, template=template)


class TelSingleStringDimensionFunction(TelTypedFunction, ABC):
    _sql_fn_name: str
    _sql_fn: func

    expected_arg_types = (
        TypeAcceptedArg('taxon', CheckStringType(), phase_range=TelPhaseRange(upper_limit=TelPhase.dimension)),
    )
    phase_spec = TelMaximumPhaseSpec(TelPhase.dimension_data_source, EXTRACT_FIRST_ARGUMENT)

    @property
    def _arg(self) -> TelExpression:
        return self._args[0]

    def result(self, context: TelRootContext) -> TelQueryResult:
        arg_result = self._arg.result(context)
        sql, template = self._sql_fn(arg_result.sql), self._sql_fn(arg_result.template_or_sql)
        return arg_result.update(sql, template=template)


class TelUpper(TelSingleStringDimensionFunction):
    """
    convert string to upper-case

    `upper` function converts provided string taxon field to upper-case string

    > Example

    ```
    upper(fb_tw_merged_objective)
    ```

    :param string taxon: string, dimension, taxon field to be converted to upper-case

    :returns string: upper-case converted provided taxon

    :raises ValidationError: `(number of arguments) != 1`
    :raises ValidationError: argument is of invalid type
    :raises ValidationError: `expression` is in an aggregation phase
    """

    """
    upper(expression: STRING): STRING
    """

    _name = 'upper'
    _sql_fn_name = 'upper'
    _sql_fn = func.upper


class TelLower(TelSingleStringDimensionFunction):
    """
    convert string to lower-case

    `lower` function converts provided string taxon field to lower-case string

    > Example

    ```
    lower(fb_tw_merged_objective)
    ```

    :param string taxon: string, dimension, taxon field to be converted to lower-case

    :returns string: lower-case converted provided taxon

    :raises ValidationError: `(number of arguments) != 1`
    :raises ValidationError: argument is of invalid type
    :raises ValidationError: `expression` is in an aggregation phase
    """

    """
    lower(expression: STRING): STRING
    """

    _name = 'lower'
    _sql_fn_name = 'lower'
    _sql_fn = func.lower


class TelTrim(TelSingleStringDimensionFunction):
    """
    strip leading and trailing whitespace (spaces, tabs and newlines)

    `trim` function strips leading and trailing whitespace (spaces, tabs and newlines)

    > Example

    ```
    trim(fb_tw_merged_objective)
    ```

    :param string taxon: string, dimension, taxon field to be converted to lower-case

    :returns string: trimmed value of the provided taxon

    :raises ValidationError: `(number of arguments) != 1`
    :raises ValidationError: argument is of invalid type
    :raises ValidationError: `expression` is in an aggregation phase
    """

    """
    trim(expression: STRING): STRING
    """

    _name = 'trim'
    _sql_fn_name = 'trim'
    _sql_fn = func.trim


class TelParse(TelTypedFunction):
    """
    split the string by a delimiter and return n-th extracted value

    `parse` function takes a string expression, a delimiter and a position of the split result, which is then returned

    > Example

    ```
    parse(fb_tw_merged_objective, "|", 2)
    ```

    :param string expression: string, dimension, taxon field, which will be split by the `delimiter`
    :param string delimiter: string expression, used to separate the expression into parts
    :param integer position: 1-based index of the part to return

    :returns string: requested part

    :raises ValidationError: `(number of arguments) != 3`
    :raises ValidationError: argument is of invalid type
    :raises ValidationError: `expression` is in an aggregation phase
    """

    """
    parse(expression: STRING, delimiter: STRING, position: INTEGER): ANY
    """

    _name = 'parse'
    expected_arg_types = (
        TypeAcceptedArg('expression', CheckStringType(), phase_range=TelPhaseRange(upper_limit=TelPhase.dimension)),
        TypeAcceptedArg('delimiter', CheckStringType(), must_be_constant=True),
        TypeAcceptedArg('position', CheckIntegerType(), must_be_constant=True),
    )
    phase_spec = TelMaximumPhaseSpec(TelPhase.dimension_data_source, EXTRACT_FIRST_ARGUMENT)
    return_type_spec = TelCopyReturnTypeSpec(
        data_type=TelDataType.ANY, argument_extractor=TelSlicingArgumentExtractor(0), is_constant=False
    )
    """
    SQL allows to do '1' + 1 = 2.. thus we return ANY, so you can do parse(...) + 1.
    Later we can always return STRING, so user should do to_number(parse()) + 1.
    """

    @property
    def _expression(self):
        return self._args[0]

    @property
    def _delimiter(self):
        return self._args[1]

    @property
    def _position(self):
        return self._args[2]

    def result(self, context: TelRootContext) -> TelQueryResult:
        expression_result = self._expression.result(context)
        delimiter_result = self._delimiter.result(context)
        position_result = self._position.result(context)

        sql, template = result_with_template(
            SplitPart, expr=expression_result, delimiter=delimiter_result, position=position_result
        )
        return TelQueryResult.merge(
            sql, context.husky_dialect, expression_result, delimiter_result, position_result, template=template
        )

    def aggregation_definition(self, context: TelRootContext) -> Optional[AggregationDefinition]:
        """
        Calculates the aggregation definition for this node. None, if it isn't possible to deduce the definition
        """
        return AggregationDefinition(type=AggregationType.group_by)


class TelContains(TelTypedFunction):
    """
    check whether provided string expression contains any of provided search constants

    `contains` function accepts a string, dimension, taxon field and one or more string constants of which at least one needs to be contained in the string expression (Note: this operation is case-sensitive)

    > Example

    ```
    contains(campaign_id, 'bud79')
    ```

    > Example for case-insensitive search, by combination with the `lower` function

    ```
    contains(lower(twitter|ad_id), 'id')
    ```

    :param string expression: string, dimension taxon to perform search on
    :param string searched_constant: (many) search constants to search in the specified `expression`

    :returns boolean: `true` if the expression contains at least one of the specified `searched_constant`s, `false` otherwise

    :raises ValidationError: `(number of arguments) < 2`
    :raises ValidationError: argument is of invalid type
    :raises ValidationError: `expression` is in an aggregation phase
    """

    """
    contains(expression: STRING, *searched_constants: STRING): BOOLEAN
    """

    _name = 'contains'
    expected_arg_types = (
        TypeAcceptedArg('expression', CheckStringType(), phase_range=TelPhaseRange(upper_limit=TelPhase.dimension)),
        VariableLengthAcceptedArg('searched_constant', CheckStringType(), must_be_constant=True),
    )
    phase_spec = TelMaximumPhaseSpec(TelPhase.dimension_data_source, EXTRACT_FIRST_ARGUMENT)
    return_type_spec = TelCopyReturnTypeSpec(
        data_type=TelDataType.BOOLEAN, argument_extractor=EXTRACT_FIRST_ARGUMENT, is_constant=False
    )
    invalid_value_spec = TelSomeArgsInvalidValueSpec(EXTRACT_FIRST_ARGUMENT)

    @property
    def _dimension_expression(self):
        return self._args[0]

    @property
    def _searched_constants(self):
        return self._args[1:]

    def plan_phase_transitions(self, context: TelRootContext) -> TelExpression:
        # Unless this is the (almost) root node, and the parent is in the aggregation phase, lift arguments to the aggregation phase as well
        if not isinstance(self.parent, TelRoot) and self.parent and self.parent.phase(context).is_metric():
            return TelContains.copy(
                self,
                [
                    TelAggregationPhase.copy(
                        self._dimension_expression, self._dimension_expression.plan_phase_transitions(context)
                    ).plan_phase_transitions(context)
                ]
                + [arg.plan_phase_transitions(context) for arg in self._searched_constants],
            )
        else:
            # Otherwise just delegate to the TelFunction.plan_phase_transitions
            return super().plan_phase_transitions(context)

    def result(self, context: TelRootContext) -> TelQueryResult:
        dimension_expression_result = self._dimension_expression.result(context)

        def clauses(sql: ClauseElement):
            return [
                sql.like(
                    literal(f'%{escape_special_character_in_like_pattern(constant)}%', String()),
                    escape=LIKE_PATTERN_ESCAPE_CHAR,
                )
                for constant in [search_literal.literal_value(context) for search_literal in self._searched_constants]
            ]

        sql, template = clauses(dimension_expression_result.sql), clauses(dimension_expression_result.template_or_sql)
        return dimension_expression_result.update(sql=or_(*sql), template=or_(*template))


class TelSingleDatetimeDimensionFunction(TelTypedFunction, ABC):
    _sql_function_template: str

    expected_arg_types = (
        TypeAcceptedArg('taxon', CheckDatetimeType(), phase_range=TelPhaseRange(upper_limit=TelPhase.dimension)),
    )
    phase_spec = TelMaximumPhaseSpec(TelPhase.dimension_data_source, EXTRACT_FIRST_ARGUMENT)

    @property
    def _arg(self) -> TelExpression:
        return self._args[0]

    @abstractmethod
    def _result(self, sql: ClauseElement) -> ClauseElement:
        pass

    def result(self, context: TelRootContext) -> TelQueryResult:
        arg_result = self._arg.result(context)
        sql, template = result_with_template(self._result, sql=arg_result)
        return arg_result.update(sql=sql, template=template)


class TelDateTruncFunction(TelTypedFunction):
    """
    return the time portion of the date time truncated to the unit

    `date_trunc` function reduces the

    > Example
    ```
    date_trunc(twitter|date, 'HOUR')
    ```

    :param datetime expression: dimension taxon field to apply the reduction to
    :param unit string: string literal, one of: HOUR, DAY, WEEK or MONTH

    :returns datetime: result of this datetime transformation function

    :raises ValidationError: `(number of arguments) != 2`
    :raises ValidationError: argument is of invalid type
    """

    """
    date_trunc(expression: DATETIME, unit: UNIT): DATETIME
    """

    _SUPPORTED_UNITS = {'HOUR', 'DAY', 'WEEK', 'MONTH'}

    _name = 'date_trunc'
    _supported_dialects = {TelDialectType.MODEL}
    expected_arg_types = (
        TypeAcceptedArg('expression', CheckDatetimeType(), phase_range=TelPhaseRange(upper_limit=TelPhase.dimension)),
        TypeAcceptedArg('unit', CheckStringType(), phase_range=TelPhaseRange(upper_limit=TelPhase.dimension)),
    )
    return_type_spec = TelCopyReturnTypeSpec(
        data_type=TelDataType.DATETIME, argument_extractor=EXTRACT_FIRST_ARGUMENT, is_constant=False
    )
    phase_spec = TelMaximumPhaseSpec(TelPhase.dimension_data_source, EXTRACT_FIRST_ARGUMENT)

    @property
    def _expr(self) -> TelExpression:
        return self._args[0]

    @property
    def _unit(self) -> TelExpression:
        return self._args[1]

    def validate(self, context: TelValidationContext) -> TelValidationContext:
        unit_value = self._unit.literal_value(context.root_context)
        if unit_value not in self._SUPPORTED_UNITS:
            context.with_error(
                f'Function {self._name} does not support time unit "{unit_value}". Supported values are: {", ".join(self._SUPPORTED_UNITS)}',
                self.location,
            )

        return super().validate(context)

    def result(self, context: TelRootContext) -> TelQueryResult:
        expr_result = self._expr.result(context)
        unit_value = self._unit.literal_value(context)
        return expr_result.update(
            sql=DateTrunc(unit_value, expr_result.sql), template=DateTrunc(unit_value, expr_result.template)
        )


class TelDateHour(TelSingleDatetimeDimensionFunction):
    """
    reduce granularity of a time taxon to hourly granularity

    `date_hour` function drops minutes and seconds of the specified datetime taxon field, for example, value `2020/05/22 09:54:23` would be converted to `2020/05/22 09:00:00`

    > Example
    ```
    date_hour(twitter|date)
    ```

    :param datetime expression: dimension taxon field to apply the reduction to

    :returns datetime: result of this datetime transformation function

    :raises ValidationError: `(number of arguments) != 1`
    :raises ValidationError: argument is of invalid type
    """

    """
    date_hour(expression: DATETIME): DATETIME
    """

    _name = 'date_hour'
    return_type_spec = TelCopyReturnTypeSpec(
        data_type=TelDataType.DATETIME, argument_extractor=EXTRACT_FIRST_ARGUMENT, is_constant=False
    )

    def _result(self, sql: ClauseElement) -> ClauseElement:
        return DateTrunc('HOUR', sql)


class TelDate(TelSingleDatetimeDimensionFunction):
    """
    reduce granularity of a time taxon to daily granularity

    `date` function drops the time component of the specified datetime taxon field, for example, value `2020/05/22 09:54:23` would be converted to `2020/05/22`

    > Example
    ```
    date(twitter|date)
    ```

    :param datetime expression: dimension taxon field to apply the reduction to

    :returns datetime: result of this datetime transformation function

    :raises ValidationError: `(number of arguments) != 1`
    :raises ValidationError: argument is of invalid type
    """

    """
    date(expression: DATETIME): DATETIME
    """

    _name = 'date'
    return_type_spec = TelCopyReturnTypeSpec(
        data_type=TelDataType.DATETIME, argument_extractor=EXTRACT_FIRST_ARGUMENT, is_constant=False
    )

    def _result(self, sql: ClauseElement) -> ClauseElement:
        return DateTrunc('DAY', sql)


class TelDateWeek(TelSingleDatetimeDimensionFunction):
    """
    reduce granularity of a time taxon to weekly granularity

    `date_week` function drops the time component of the specified datetime taxon field, and returns the date of the beginning of the week date, for example, value `2020/05/22 09:54:23` would be converted to `2020/05/17`

    > Example
    ```
    date_week(twitter|date)
    ```

    :param datetime expression: dimension taxon field to apply the reduction to

    :returns datetime: result of this datetime transformation function

    :raises ValidationError: `(number of arguments) != 1`
    :raises ValidationError: argument is of invalid type
    """

    """
    date_week(expression: DATETIME): DATETIME
    """

    _name = 'date_week'
    return_type_spec = TelCopyReturnTypeSpec(
        data_type=TelDataType.DATETIME, argument_extractor=EXTRACT_FIRST_ARGUMENT, is_constant=False
    )

    def _result(self, sql: ClauseElement) -> ClauseElement:
        return DateTrunc('WEEK', sql)


class TelDateMonth(TelSingleDatetimeDimensionFunction):
    """
    reduce granularity of a time taxon to monthly granularity

    `date_month` function drops the time component of the specified datetime taxon field, and returns the date of the beginning of the month date, for example, value `2020/05/22 09:54:23` would be converted to `2020/05/01`

    > Example
    ```
    date_month(twitter|date)
    ```

    :param datetime expression: dimension taxon field to apply the reduction to

    :returns datetime: result of this datetime transformation function

    :raises ValidationError: `(number of arguments) != 1`
    :raises ValidationError: argument is of invalid type
    """

    """
    date_month(expression: DATETIME): DATETIME
    """

    _name = 'date_month'
    return_type_spec = TelCopyReturnTypeSpec(
        data_type=TelDataType.DATETIME, argument_extractor=EXTRACT_FIRST_ARGUMENT, is_constant=False
    )

    def _result(self, sql: ClauseElement) -> ClauseElement:
        return DateTrunc('MONTH', sql)


class TelHourOfDay(TelSingleDatetimeDimensionFunction):
    """
    return the hour of day associated with the datetime taxon field

    `hour_of_day` function returns the hour of the day of the provided datetime taxon field

    > Example
    ```
    hour_of_day(twitter|date)
    ```

    :param datetime expression: dimension taxon field

    :returns integer: the hour of the day associated with the datetime value

    :raises ValidationError: `(number of arguments) != 1`
    :raises ValidationError: argument is of invalid type
    """

    """
    hour_of_day(expression: DATETIME): INTEGER
    """

    _name = 'hour_of_day'
    return_type_spec = TelCopyReturnTypeSpec(
        data_type=TelDataType.INTEGER, argument_extractor=EXTRACT_FIRST_ARGUMENT, is_constant=False
    )

    def _result(self, sql: ClauseElement) -> ClauseElement:
        return Extract('HOUR', sql)


class TelDayOfWeek(TelSingleDatetimeDimensionFunction):
    """
    return the day of week associated with the datetime taxon field

    `day_of_week` function returns the day of week of the provided datetime taxon field

    > Example
    ```
    day_of_week(twitter|date)
    ```

    :param datetime expression: dimension taxon field

    :returns integer: the day of the week associated with the datetime value

    :raises ValidationError: `(number of arguments) != 1`
    :raises ValidationError: argument is of invalid type
    """

    """
    day_of_week(expression: DATETIME): INTEGER
    """

    _name = 'day_of_week'
    return_type_spec = TelCopyReturnTypeSpec(
        data_type=TelDataType.INTEGER, argument_extractor=EXTRACT_FIRST_ARGUMENT, is_constant=False
    )

    def _result(self, sql: ClauseElement) -> ClauseElement:
        return Extract('DOW', sql)


class TelWeekOfYear(TelSingleDatetimeDimensionFunction):
    """
    return the week of the year associated with the datetime taxon field

    `day_of_week` function returns the week of year of the provided datetime taxon field

    > Example
    ```
    week_of_year(twitter|date)
    ```

    :param datetime expression: dimension taxon field

    :returns integer: the week of the year associated with the datetime value

    :raises ValidationError: `(number of arguments) != 1`
    :raises ValidationError: argument is of invalid type
    """

    """
    week_of_year(expression: DATETIME): INTEGER
    """

    _name = 'week_of_year'
    return_type_spec = TelCopyReturnTypeSpec(
        data_type=TelDataType.INTEGER, argument_extractor=EXTRACT_FIRST_ARGUMENT, is_constant=False
    )

    def _result(self, sql: ClauseElement) -> ClauseElement:
        return Extract('WEEK', sql)


class TelMonthOfYear(TelSingleDatetimeDimensionFunction):
    """
    return the month of the year associated with the datetime taxon field

    `month_of_year` function returns the month of year of the provided datetime taxon field

    > Example
    ```
    month_of_year(twitter|date)
    ```

    :param datetime expression: dimension taxon field

    :returns integer: the month of the year associated with the datetime value

    :raises ValidationError: `(number of arguments) != 1`
    :raises ValidationError: argument is of invalid type
    """

    """
    month_of_year(expression: DATETIME): INTEGER
    """

    _name = 'month_of_year'
    return_type_spec = TelCopyReturnTypeSpec(
        data_type=TelDataType.INTEGER, argument_extractor=EXTRACT_FIRST_ARGUMENT, is_constant=False
    )

    def _result(self, sql: ClauseElement) -> ClauseElement:
        return Extract('MONTH', sql)


class TelYear(TelSingleDatetimeDimensionFunction):
    """
    return the year associated with the datetime taxon field

    `year` function returns the year of the provided datetime taxon field

    > Example
    ```
    year(twitter|date)
    ```

    :param datetime expression: dimension taxon field

    :returns integer: the year associated with the datetime value

    :raises ValidationError: `(number of arguments) != 1`
    :raises ValidationError: argument is of invalid type
    """

    """
    year(expression: DATETIME): INTEGER
    """

    _name = 'year'
    return_type_spec = TelCopyReturnTypeSpec(
        data_type=TelDataType.INTEGER, argument_extractor=EXTRACT_FIRST_ARGUMENT, is_constant=False
    )

    def _result(self, sql: ClauseElement) -> ClauseElement:
        return Extract('YEAR', sql)


class TelToText(TelTypedFunction):
    """
    cast the value to a string data type

    `to_text` function accepts any expression that is cast to string type

    > Example

    ```
    to_text(twitter|date)
    ```

    :param any expression: any expression to be cast to the required type

    :returns string: string representation of the value in the `expression` field

    :raises ValidationError: `(number of arguments) != 1`
    :raises ValidationError: argument is of invalid type
    """

    """
    to_text(expression: ANY): STRING
    """

    _name = 'to_text'
    expected_arg_types = (AnyTypeAcceptedArg('expression'),)
    phase_spec = TelMinimumPhaseSpec(TelPhase.dimension, EXTRACT_FIRST_ARGUMENT)
    return_type_spec = TelCopyReturnTypeSpec(
        data_type=TelDataType.STRING, argument_extractor=EXTRACT_FIRST_ARGUMENT, is_constant=False
    )

    @property
    def _arg(self) -> TelExpression:
        return self._args[0]

    def rewrite(self, callback: Callable[[TelExpression], TelExpression], context: TelRootContext) -> TelExpression:
        """
        Avoid performing unnecessary type casting
        """
        # no need for casting, if the argument is already string
        if self._arg.return_type(context).is_string():
            return self._arg.rewrite(callback, context)
        else:
            # otherwise, use the original node
            return TelToText.copy(self, [callback(self._arg.rewrite(callback, context))])

    def result(self, context: TelRootContext) -> TelQueryResult:
        arg_result = self._arg.result(context)
        sql, template = result_with_template(lambda sql: sqlalchemy.cast(sql, String()), sql=arg_result)
        return arg_result.update(sql=sql, template=template)

    def aggregation_definition(self, context: TelRootContext) -> Optional[AggregationDefinition]:
        """
        Calculates the aggregation definition for this node. None, if it isn't possible to deduce the definition
        """
        return AggregationDefinition(type=AggregationType.group_by)


class TelToBool(TelTypedFunction):
    """
    cast the value to a boolean data type

    `to_bool` function accepts number, string or boolean expression that is cast to boolean type. If the argument is a number, then 0 is converted to `false`, all other numbers are `true`.
    If the argument is a string equal to `false` (case-insensitive), then it's a `false`, otherwise the result is `true`

    > Example

    ```
    to_bool(facebook_ads|done)
    ```

    :param number_string_boolean expression: an expression to be cast to the required type

    :returns boolean: boolean representation of the value in the `expression` field

    :raises ValidationError: `(number of arguments) != 1`
    :raises ValidationError: argument is of invalid type
    """

    """
    to_bool(expression: NUMBER|STRING|BOOLEAN): BOOLEAN
    """

    _name = 'to_bool'
    expected_arg_types = (TypeAcceptedArg('expression', CheckNumberOrStringOrBooleanType()),)
    phase_spec = TelMinimumPhaseSpec(TelPhase.dimension, EXTRACT_FIRST_ARGUMENT)
    return_type_spec = TelCopyReturnTypeSpec(
        data_type=TelDataType.BOOLEAN, argument_extractor=EXTRACT_FIRST_ARGUMENT, is_constant=False
    )

    @property
    def _arg(self) -> TelExpression:
        return self._args[0]

    def rewrite(self, callback: Callable[[TelExpression], TelExpression], context: TelRootContext) -> TelExpression:
        """
        Avoid performing unnecessary type casting
        """
        arg_return_type = self._arg.return_type(context)
        # no need for casting, if the argument is already boolean
        if arg_return_type.is_boolean():
            return self._arg.rewrite(callback, context)
        # This is for the model TEL dialect, where types are unknown and unchecked
        elif arg_return_type.data_type == TelDataType.UNKNOWN:
            return TelToBool.copy(self, [callback(self._arg.rewrite(callback, context))])
        else:
            # otherwise, transform the casting to the correct IF-statement
            if arg_return_type.is_number():
                # numbers need to be casted using CASE-when NUMBER = 0
                tel_case = TelIff.copy(
                    self,
                    [
                        TelLogicalOperation.copy(self._arg, '=', self._arg, TelInteger.copy(self._arg, 0)),
                        TelBoolean.copy(self._arg, False),
                        TelBoolean.copy(self._arg, True),
                    ],
                )
            else:
                # strings need to be casted using CASE-when STRING.lower() = 'false'
                tel_case = TelIff.copy(
                    self,
                    [
                        TelLogicalOperation.copy(
                            self._arg, '=', TelLower.copy(self._arg, [self._arg]), TelString.copy(self._arg, 'false')
                        ),
                        TelBoolean.copy(self._arg, False),
                        TelBoolean.copy(self._arg, True),
                    ],
                )

            return TelToBool.copy(self, [callback(tel_case.rewrite(callback, context))])

    def result(self, context: TelRootContext) -> TelQueryResult:
        result = self._arg.result(context)

        if self._arg.return_type(context).is_boolean():
            return result
        else:
            sql, template = result_with_template(lambda sql: sqlalchemy.cast(sql, Boolean()), sql=result)
            return result.update(sql=sql, template=template)

    def aggregation_definition(self, context: TelRootContext) -> Optional[AggregationDefinition]:
        """
        Calculates the aggregation definition for this node. None, if it isn't possible to deduce the definition
        """
        return AggregationDefinition(type=AggregationType.group_by)


class TelToNumber(TelTypedFunction):
    """
    cast the value to a number data type

    `to_number` function accepts any expression that is cast to a number type, integer or float, depending whether precision is specified or not

    > Example

    ```
    to_number(facebook_ads|done)
    ```

    > Example, with a precision specified

    ```
    to_number(facebook_ads|done, 2)
    ```

    :param number_string_boolean expression: an expression to be cast to the required type
    :param integer precision: (optional) when precision is specified, the result is cut off at this precision level (default value is 0)

    :returns number: integer or float representation of the value in the `expression` field

    :raises ValidationError: `(number of arguments) > 2`
    :raises ValidationError: argument is of invalid type
    """

    """
    to_number(expression: NUMBER|STRING|BOOLEAN, [precision: INTEGER]): INTEGER|NUMBER
    """

    _name = 'to_number'
    expected_arg_types = (
        TypeAcceptedArg('expression', CheckNumberOrStringOrBooleanType()),
        TypeAcceptedArg('precision', CheckIntegerType(), optional=True, must_be_constant=True),
    )
    phase_spec = TelMaximumPhaseSpec(TelPhase.metric_pre, EXTRACT_FIRST_ARGUMENT)
    return_type_spec = TelCopyReturnTypeSpec(
        data_type=TelDataType.NUMERIC, argument_extractor=EXTRACT_FIRST_ARGUMENT, is_constant=False
    )

    @property
    def _expression(self) -> TelExpression:
        return self._args[0]

    @property
    def _precision(self) -> Optional[TelExpression]:
        return self._args[1] if len(self._args) > 1 else None

    def return_type(self, context: TelRootContext) -> TelType:
        data_type = TelDataType.NUMERIC if self._precision else TelDataType.INTEGER
        return self._expression.return_type(context).copy(data_type=data_type)

    def rewrite(self, callback: Callable[[TelExpression], TelExpression], context: TelRootContext) -> TelExpression:
        """
        Avoid performing unnecessary type casting
        """
        expr_return_type = self._expression.return_type(context)
        if expr_return_type.is_integer() and not self._precision:
            # no need for casting, if the argument is already integer
            return self._expression.rewrite(callback, context)
        elif expr_return_type.is_number() and not self._precision:
            # no need for casting, if the argument is already number
            return self._expression.rewrite(callback, context)
        else:
            # otherwise, transform the casting to the correct IF-statement
            if expr_return_type.is_boolean():
                # numbers need to be casted using CASE-when NUMBER = 0
                inner_node = TelIff.copy(
                    self,
                    [
                        TelLogicalOperation.copy(
                            self._expression, '=', self._expression, TelBoolean.copy(self._expression, True)
                        ),
                        TelInteger.copy(self._expression, 1),
                        TelInteger.copy(self._expression, 0),
                    ],
                )
            else:
                inner_node = self._expression

            if self._precision:
                return TelToNumber.copy(
                    self,
                    [
                        callback(inner_node.rewrite(callback, context)),
                        callback(self._precision.rewrite(callback, context)),
                    ],
                )
            else:
                return TelToNumber.copy(self, [callback(inner_node.rewrite(callback, context))])

    def result(self, context: TelRootContext) -> TelQueryResult:
        expr_result = self._expression.result(context)
        precision_result = self._precision.literal_value(context) if self._precision else None
        sql, template = result_with_template(
            lambda sql: sqlalchemy.cast(
                sql, DECIMAL(precision=16, scale=precision_result) if precision_result else Numeric(16)
            ),
            sql=expr_result,
        )
        return expr_result.update(sql=sql, template=template)

    def aggregation_definition(self, context: TelRootContext) -> Optional[AggregationDefinition]:
        """
        Calculates the aggregation definition for this node. None, if it isn't possible to deduce the definition
        """
        return AggregationDefinition(type=AggregationType.sum)


class TelToDate(TelTypedFunction):
    """
    cast the value to a date data type

    `to_date` function accepts number, string or date or datetime expression that is cast to date type,
    using a format string to parse the input expression, unless it's a number expression

    > Example

    ```
    to_date(ad_name, 'YYYY-MM-DD')
    ```

    :param number_string_date_datetime expression: an expression to be cast to the required type
    :param string format: (optional) format of the expression if it's a string

    :returns date: date representation of the value in the `expression` field

    :raises ValidationError: `(number of arguments) != 1 or 2`
    :raises ValidationError: argument is of invalid type
    """

    """
    to_date(expression: NUMBER|STRING|DATE|DATETIME, [format: STRING]): DATE
    """

    _name = 'to_date'
    expected_arg_types = (
        TypeAcceptedArg('expression', CheckNumberOrStringOrDateType()),
        TypeAcceptedArg('format', CheckStringType(), optional=True, must_be_constant=True),
    )
    phase_spec = TelMinimumPhaseSpec(TelPhase.dimension, EXTRACT_FIRST_ARGUMENT)
    return_type_spec = TelCopyReturnTypeSpec(
        data_type=TelDataType.DATETIME, argument_extractor=EXTRACT_FIRST_ARGUMENT, is_constant=False
    )

    @property
    def _expression(self) -> TelExpression:
        return self._args[0]

    @property
    def _format(self) -> Optional[TelExpression]:
        return self._args[1] if len(self._args) > 1 else None

    def validate(self, context: TelValidationContext) -> TelValidationContext:
        expression = self._expression
        date_format = self._format

        expression_return_type = expression.return_type(context.root_context)

        if not expression_return_type.is_number() and not date_format:
            return context.with_error(
                f'Argument 2 in function {self._name} is required, if the first argument is not a number',
                location=expression.location,
            )

        if date_format and expression_return_type.is_number():
            return context.with_error(
                f'Argument 2 in function {self._name} is allowed only if the first argument is not a number',
                location=date_format.location,
            )

        return super().validate(context)

    def rewrite(self, callback: Callable[[TelExpression], TelExpression], context: TelRootContext) -> TelExpression:
        """
        Avoid performing unnecessary type casting
        """
        if self._expression.return_type(context).is_date():
            # no need for casting, if the argument is already date
            return self._expression.rewrite(callback, context)
        else:
            # otherwise, use the original node
            return super().rewrite(callback, context)

    def result(self, context: TelRootContext) -> TelQueryResult:
        expr_result = self._expression.result(context)
        date_format_result = self._format.result(context) if self._format else None
        sql, template = result_with_template(ParseDate, expr=expr_result, format=date_format_result)
        return expr_result.update(sql=sql, template=template)

    def aggregation_definition(self, context: TelRootContext) -> Optional[AggregationDefinition]:
        """
        Calculates the aggregation definition for this node. None, if it isn't possible to deduce the definition
        """
        return AggregationDefinition(type=AggregationType.group_by)


class TelDateDiff(TelTypedFunction):
    """
    calculate difference between two datetime values, in the specified time unit

    `date_diff` function accepts time unit, in which the difference between two date time values is returned. Start and end values must be from the same data source and they must not be in the aggregation phase or later

    > Example
    ```
    date_diff('MINUTE', twitter|date, merged_date)
    ```

    :param string time_unit: one of supported time unit values (supported values are: `"SECOND"`, `"MINUTE"`, `"HOUR"`, `"DAY"`, `"WEEK"`, `"MONTH"`, `"YEAR"`
    :param datetime start_time: start time for calculation of time difference
    :param datetime end_time: end time for calculation of time difference

    :returns integer: the difference between two dates, in the unit specified by the `time_unit` argument

    :raises ValidationError: `(number of arguments) != 3`
    :raises ValidationError: argument is of invalid type
    :raises ValidationError: `time_unit` is an unknown time unit value
    :raises ValidationError: `start_time` or `end_time` is in an aggregation phase
    :raises ValidationError: `start_time` and `end_time` are from different data sources
    """

    """
    date_diff(time_unit: STRING, start_time: DATETIME, end_time: DATETIME): INTEGER
    """

    _name = 'date_diff'
    expected_arg_types = (
        TypeAcceptedArg('time_unit', CheckStringType(), must_be_constant=True),
        TypeAcceptedArg('start_time', CheckDatetimeType(), phase_range=TelPhaseRange(upper_limit=TelPhase.dimension)),
        TypeAcceptedArg('end_time', CheckDatetimeType(), phase_range=TelPhaseRange(upper_limit=TelPhase.dimension)),
    )
    _argument_extractor = TelSlicingArgumentExtractor(1)
    phase_spec = TelMaximumPhaseSpec(TelPhase.dimension_data_source, _argument_extractor)
    return_type_spec = TelCopyReturnTypeSpec(argument_extractor=_argument_extractor, is_constant=False)

    @property
    def _time_unit(self) -> TelExpression:
        return self._args[0]

    def _time_unit_parsed(self, context: TelRootContext):
        return self._time_unit.literal_value(context)

    @property
    def _start_time(self) -> TelExpression:
        return self._args[1]

    @property
    def _end_time(self) -> TelExpression:
        return self._args[2]

    def validate(self, context: TelValidationContext) -> TelValidationContext:
        super().validate(context)

        if context.has_errors:
            return context

        non_global_data_sources = {ds for ds in self.return_data_sources(context.root_context) if ds is not None}
        if len(non_global_data_sources) > 1:
            return context.with_error(
                'date_diff accepts only taxons from the same data source or after merge() is applied',
                location=self.location,
            )

        return context

    def plan_phase_transitions(self, context: TelRootContext) -> 'TelExpression':
        # Move arguments to dimension phase if the result is dimension phase
        if self.phase(context) is TelPhase.dimension:
            return TelDateDiff.copy(
                self,
                [
                    self._time_unit.plan_phase_transitions(context),
                    TelDimensionPhase.copy(self._start_time, self._start_time.plan_phase_transitions(context)),
                    TelDimensionPhase.copy(self._end_time, self._end_time.plan_phase_transitions(context)),
                ],
            )
        else:
            # Otherwise do nothing
            return super().plan_phase_transitions(context)

    def result(self, context: TelRootContext) -> TelQueryResult:
        start_time_result = self._start_time.result(context)
        end_time_result = self._end_time.result(context)

        sql, template = result_with_template(
            lambda start, end: TimestampDiff(self._time_unit_parsed(context), start, end),
            start=start_time_result,
            end=end_time_result,
        )
        return TelQueryResult.merge(sql, context.husky_dialect, start_time_result, end_time_result, template=template)


class TelOverride(TelTypedFunction):
    """
    overrides original value

    `override` changes values in TEL expression from the first argument to new values using mapping specified in the second argument

    > Example (excluding missing values)

    ```
    override(gender, 'our-gender-mapping', false)
    ```

    > Example (including missing values)

    ```
    override(gender, 'our-gender-mapping')
    ```

    :param string original_dimension: Original dimension that we want to override
    :param string override_mapping_slug: Unique identification of the override mapping
    :param string include_missing_values: Controls whether values not present in the mapping should be part of the output under 'Unknown' value (default value is True)

    :returns string: value of the override mapping

    :raises ValidationError: `(number of arguments) != 2 or 3`
    :raises ValidationError: argument is of invalid type
    :raises ValidationError: `original_dimension` not a dimension
    """

    """
    override(original_dimension: STRING, override_mapping_slug: DATETIME, [include_missing_values: BOOLEAN]): STRING
    """

    _name = 'override'
    _supported_dialects = {TelDialectType.TAXON}
    expected_arg_types = (
        TypeAcceptedArg(
            'original_dimension', CheckStringType(), phase_range=TelPhaseRange(upper_limit=TelPhase.dimension)
        ),
        TypeAcceptedArg('override_mapping_slug', CheckStringType(), must_be_constant=True),
        TypeAcceptedArg('include_missing_values', CheckBooleanType(), must_be_constant=True, optional=True),
    )
    phase_spec = TelFixedPhaseSpec(TelPhase.dimension)
    return_type_spec = TelFixedReturnTypeSpec(data_type=TelDataType.STRING, is_constant=False)

    UNKNOWN_VALUE = 'Unknown'
    """Constant representing unknown value"""

    @property
    def _original_dimension(self) -> TelExpression:
        return self._args[0]

    @property
    def _override_mapping_slug(self) -> TelExpression:
        return self._args[1]

    @property
    def _include_missing_values_arg(self) -> Optional[TelExpression]:
        return self._args[2] if len(self._args) == 3 else None

    @property
    def _should_include_missing_values(self) -> bool:
        arg = self._include_missing_values_arg
        if arg:
            # very ugly way of getting the actual value
            return cast(TelBoolean, arg)._value
        else:
            return True

    def validate(self, context: TelValidationContext) -> TelValidationContext:
        if self._include_missing_values_arg and not isinstance(self._include_missing_values_arg, TelBoolean):
            context.with_error(
                f'Argument 3 in function {self._name} must be a boolean constant',
                location=self._include_missing_values_arg.location,
            )

        return super().validate(context)

    def plan_phase_transitions(self, context: TelRootContext) -> TelExpression:
        # Move the argument to the dimension phase
        args = [
            TelDimensionPhase.copy(
                self._override_mapping_slug, self._original_dimension.plan_phase_transitions(context)
            ),
            self._override_mapping_slug,
        ]
        include_missing_values = self._include_missing_values_arg
        if include_missing_values:
            args.append(include_missing_values)

        return TelOverride.copy(self, args)

    def result(self, context: TelRootContext) -> TelQueryResult:
        original_dimension_result = self._original_dimension.result(context)
        should_include_missing_values = self._should_include_missing_values

        # generate the correct SQL identifier of the mapping
        override_mapping_slug = cast(str, self._override_mapping_slug.literal_value(context))
        override_identifier = OverrideMappingSql.generate_identifier(
            compile_query(original_dimension_result.sql, context.husky_dialect),
            override_mapping_slug,
            should_include_missing_values,
        )

        # generate the SQL to access the column
        override_sql = literal_column(
            f'{safe_quote_identifier(override_identifier, context.husky_dialect)}.{OverrideMappingSql.CHANGED_COLUMN_NAME}'
        )
        # NOTE: We are not using TelCoalesce here on purpose - it would move the calculation to metric_post phase :(
        if should_include_missing_values:
            column_sql = functions.coalesce(override_sql, literal('Unknown', String()))
        else:
            column_sql = override_sql

        # NOTE: again, not using TelIff here in order to avoid moving the calculation to metric_post phase
        sql = case([(override_sql == literal(OverrideMappingSql.PANO_NULL, String()), null())], else_=column_sql)

        # gather information about Override mapping
        tel_info = OverrideMappingTelInformation(
            compile_query(original_dimension_result.sql, context.husky_dialect),
            override_mapping_slug,
            should_include_missing_values,
        )
        return original_dimension_result.update(sql, override_mappings={tel_info}, template=sql)


class TelCumulative(TelTypedFunction):
    """
    cumulative window function

    `cumulative` calculates aggregated values using cumulative window frame. Values are divided into windows using all request dimensions excluding the time dimension,
    which is used to order the values. Aggregation type is derived automatically.

    > Example

    ```
    cumulative(spend, date)
    ```

    :param numeric metric: metric to apply cumulative aggregation to
    :param datetime time_dimension: time dimension to order the values by

    :returns numeric: cumulated metric value for each row

    :raises ValidationError: `(number of arguments) != 2`
    :raises ValidationError: argument is of invalid type
    :raises ValidationError: `time_dimension` is not a dimension
    :raises ValidationError: aggregation type couldn't be derived or is not supported
    """

    _name = 'cumulative'
    _supported_dialects = {TelDialectType.TAXON}
    expected_arg_types = (
        TypeAcceptedArg('metric', CheckNumberType()),
        TypeAcceptedArg(
            'time_dimension', CheckDatetimeType(), phase_range=TelPhaseRange(upper_limit=TelPhase.dimension)
        ),
    )
    phase_spec = TelFixedPhaseSpec(TelPhase.metric_post)
    return_type_spec = TelCopyReturnTypeSpec(argument_extractor=EXTRACT_FIRST_ARGUMENT, is_constant=False)

    @property
    def _metric(self) -> TelExpression:
        return self._args[0]

    @property
    def _time_dimension(self) -> TelExpression:
        return self._args[1]

    def aggregation_definition(self, context: TelRootContext) -> Optional[AggregationDefinition]:
        return self._metric.aggregation_definition(context)

    def plan_phase_transitions(self, context: TelRootContext) -> TelExpression:
        return TelCumulative.copy(
            self,
            [
                TelPostAggregationPhase.copy(
                    self._metric, self._metric.plan_phase_transitions(context), self.aggregation_definition(context)
                ),
                TelPostAggregationPhase.copy(
                    self._time_dimension,
                    TelAggregationPhase.copy(
                        self._time_dimension, self._time_dimension.plan_phase_transitions(context)
                    ).plan_phase_transitions(context),
                    AggregationDefinition(type=AggregationType.group_by),
                ),
            ],
        )

    def result(self, context: TelRootContext) -> TelQueryResult:
        metric_result = self._metric.result(context)
        time_dimension_result = self._time_dimension.result(context)

        agg_function = func.SUM
        metric_column = metric_result.sql
        order_by_column = time_dimension_result.sql
        window_function = agg_function(metric_column).over(
            partition_by=text(f'${{{PostFormula.DIMENSION_SLUGS_TEMPLATE_PARAM}}}'),
            order_by=order_by_column,
            rows=(None, 0),
        )
        window_function_no_partition = agg_function(metric_column).over(order_by=order_by_column, rows=(None, 0))

        time_dimension_slug = compile_query(time_dimension_result.sql, context.husky_dialect)
        time_dimension_used_slugs = self._time_dimension.used_taxons(context).all_taxons.keys()

        return TelQueryResult.merge(
            window_function_no_partition, context.husky_dialect, metric_result, time_dimension_result
        ).update(template=window_function, excluded_slugs={time_dimension_slug} | time_dimension_used_slugs)

    def rewrite(self, callback: Callable[[TelExpression], TelExpression], context: TelRootContext) -> TelExpression:
        """
        If the metric expression contains division of two taxons, then the cumulative function is applied
        to each operand instead of the division result, for example:

        cumulative(spend, date)               -> cumulative(spend, date)
        cumulative(spend / 1000, date)        -> cumulative(spend / 1000, date)
        cumulative(spend / impressions, date) -> cumulative(spend, date) / cumulative(impressions, date)
        """
        if not self._metric.fold(TelCumulative._should_rewrite, False, context):
            return super().rewrite(callback, context)

        transform_cumulative = lambda e: transform(callback(e))
        optimized_time_dimension = self._time_dimension.rewrite(callback, context)

        def transform(expr: TelExpression) -> TelExpression:
            if isinstance(expr, TelDivision):
                division = cast(TelDivision, expr)
                if (
                    not division.left.return_type(context).is_constant
                    and not division.right.return_type(context).is_constant
                ):
                    left = division.left.rewrite(transform_cumulative, context)
                    right = division.right.rewrite(transform_cumulative, context)

                    new_left = TelCumulative.copy(left, [left, optimized_time_dimension])
                    new_right = TelCumulative.copy(right, [right, optimized_time_dimension])

                    return division.copy(division, division.op, new_left, new_right)

            return expr

        return transform_cumulative(self._metric.rewrite(transform_cumulative, context))

    @staticmethod
    def _should_rewrite(expr: TelExpression, acc: bool, context: TelRootContext) -> bool:
        """
        Returns True if the TelExpresssion should be optimized otherwise returns initial value.
        """
        if isinstance(expr, TelDivision):
            division = cast(TelDivision, expr)
            left_constant = division.left.return_type(context).is_constant
            right_constant = division.right.return_type(context).is_constant

            return acc or (not left_constant and not right_constant)
        else:
            return acc


class TelOverall(TelTypedFunction):
    """
    overall window function

    `overall` calculates aggregated values for each row from the entire result. Aggregation type is derived automatically.

    > Example

    ```
    overall(spend)
    ```

    :param numeric metric: metric to apply aggregation to

    :returns numeric: aggregated metric value for each row

    :raises ValidationError: `(number of arguments) != 1`
    :raises ValidationError: argument is of invalid type
    :raises ValidationError: aggregation type couldn't be derived or is not supported
    """

    _name = 'overall'
    _supported_dialects = {TelDialectType.TAXON}
    expected_arg_types = (TypeAcceptedArg('metric', CheckNumberType()),)
    phase_spec = TelFixedPhaseSpec(TelPhase.metric_post)
    return_type_spec = TelCopyReturnTypeSpec(argument_extractor=EXTRACT_FIRST_ARGUMENT, is_constant=False)

    @property
    def _metric(self) -> TelExpression:
        return self._args[0]

    def aggregation_definition(self, context: TelRootContext) -> Optional[AggregationDefinition]:
        return self._metric.aggregation_definition(context)

    def plan_phase_transitions(self, context: TelRootContext) -> TelExpression:
        return TelOverall.copy(
            self,
            [
                TelPostAggregationPhase.copy(
                    self._metric, self._metric.plan_phase_transitions(context), self.aggregation_definition(context)
                )
            ],
        )

    def result(self, context: TelRootContext) -> TelQueryResult:
        metric_result = self._metric.result(context)
        sql, template = result_with_template(lambda sql: func.SUM(sql).over(), sql=metric_result)
        return TelQueryResult.merge(sql, context.husky_dialect, metric_result, template=template)


class TelNow(TelTypedFunction):
    """
    current date and time

    `now` returns current date and time

    > Example

    ```
    now()
    ```

    :returns datetime: current date and time
    """

    _name = 'now'
    expected_arg_types: Iterable[AcceptedArg] = []
    phase_spec = TelFixedPhaseSpec(TelPhase.any)
    return_type_spec = TelFixedReturnTypeSpec(data_type=TelDataType.DATETIME, is_constant=False)

    def result(self, context: TelRootContext) -> TelQueryResult:
        return TelQueryResult(functions.now(), context.husky_dialect, template=functions.now())


TEL_FUNCTIONS: Mapping[str, Type[TelFunction]] = {
    'coalesce': TelCoalesce,
    'iff': TelIff,
    'ifs': TelIfs,
    'concat': TelConcat,
    'merge': TelMerge,
    'convert_timezone': TelConvertTimeZone,
    'upper': TelUpper,
    'lower': TelLower,
    'trim': TelTrim,
    'parse': TelParse,
    'contains': TelContains,
    'date_trunc': TelDateTruncFunction,
    'date_hour': TelDateHour,
    'date': TelDate,
    'date_week': TelDateWeek,
    'date_month': TelDateMonth,
    'hour_of_day': TelHourOfDay,
    'day_of_week': TelDayOfWeek,
    'week_of_year': TelWeekOfYear,
    'month_of_year': TelMonthOfYear,
    'year': TelYear,
    'to_bool': TelToBool,
    'to_date': TelToDate,
    'to_text': TelToText,
    'to_number': TelToNumber,
    'date_diff': TelDateDiff,
    'override': TelOverride,
    'cumulative': TelCumulative,
    'overall': TelOverall,
    'now': TelNow,
}
