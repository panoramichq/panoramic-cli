from abc import ABC
from enum import Enum
from functools import total_ordering
from typing import Any, Dict, Iterable, Optional

from panoramic.cli.husky.core.tel.tel_remote_interface import (
    TelArgumentExtractor,
    TelExpressionSpecGetter,
    TelRemoteSpecProtocol,
)


@total_ordering
# https://stackoverflow.com/a/39269589/1299412
class TelPhase(Enum):
    """
    Enum depicting various TEL phases.
    Starts with dimension phase, followed by metric_pre and then metric_post phases.
    The value on enum is an int, increasing in the order of how tel phases are executed. Math Max function can then be
    used to final the latest phase.
    """

    any = 0
    """
    For formulas that can be executed in any phase. Better than having None.
    """
    dimension_data_source = 1
    dimension = 2
    metric_pre = 3
    metric_post = 4

    blending_projection = 5
    """
    The final projection phase applied to the query in its topmost level.
    """

    def is_metric(self):
        return self in [self.metric_pre, self.metric_post]

    def is_dimension(self):
        return self in [self.dimension, self.dimension_data_source]

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented

    @staticmethod
    def max(phases: Iterable['TelPhase']) -> 'TelPhase':
        if phases:
            return TelPhase(max([phase.value for phase in phases]))

        return TelPhase.any

    @property
    def graphviz_fillcolor(self):
        color = "#E2F89C"
        if self == TelPhase.dimension_data_source:
            color = "#D5E1A3"
        elif self == TelPhase.dimension:
            color = "#BDC4A7"
        elif self == TelPhase.metric_pre:
            color = "#95AFBA"
        elif self == TelPhase.metric_post:
            color = "#3F7CAC"

        return color


class TelPhaseRange(TelRemoteSpecProtocol):
    def __init__(self, lower_limit=TelPhase.any, upper_limit=TelPhase.blending_projection):
        self.min_accepted_phase = lower_limit
        self.max_accepted_phase = upper_limit

    def __contains__(self, item: TelPhase):
        return self.min_accepted_phase <= item <= self.max_accepted_phase

    def __str__(self):
        if self.min_accepted_phase == TelPhase.any:
            return f'lower or equal than {self.max_accepted_phase.name}'
        elif self.max_accepted_phase == TelPhase.blending_projection:
            return f'greater or equal than {self.min_accepted_phase.name}'
        else:
            return f'between {self.min_accepted_phase.name} and {self.max_accepted_phase.name}'

    def to_remote_spec(self) -> Dict[str, Any]:
        return {"minAcceptedPhase": self.min_accepted_phase.value, "maxAcceptedPhase": self.max_accepted_phase.value}


ANY_PHASE = TelPhaseRange()


class PhaseSpecKind(Enum):
    FIXED = "fixed"
    MINIMUM = "minimum"
    MAXIMUM = "maximum"


class TelPhaseSpec(TelRemoteSpecProtocol, TelExpressionSpecGetter[TelPhase], ABC):
    pass


class TelFixedPhaseSpec(TelPhaseSpec):
    """
    Fixed, stable phase, nothing to compute from function arguments, or otherwise.
    """

    def __init__(self, phase: TelPhase):
        self._phase = phase

    def to_remote_spec(self) -> Dict[str, Any]:
        return {"kind": PhaseSpecKind.FIXED, "phase": self._phase.value}

    def get(self, args, context) -> TelPhase:
        return self._phase


class TelMaximumPhaseSpec(TelPhaseSpec):
    """
    Phase computed from function arguments, while providing the lower bound (minimal) phase.
    """

    def __init__(
        self, minimum_phase: TelPhase = TelPhase.any, argument_extractor: Optional[TelArgumentExtractor] = None
    ):
        self._minimum_phase = minimum_phase
        self._argument_extractor = argument_extractor

    def to_remote_spec(self) -> Dict[str, Any]:
        result = {"kind": PhaseSpecKind.MAXIMUM, "noLowerThan": self._minimum_phase.value}

        if self._argument_extractor:
            result["argumentExtractor"] = self._argument_extractor.to_remote_spec()

        return result

    def get(self, args, context) -> TelPhase:
        if self._argument_extractor:
            extracted_arguments = self._argument_extractor.extract_arguments(args)
            return TelPhase.max([self._minimum_phase] + [arg.phase(context) for arg in extracted_arguments])
        else:
            return self._minimum_phase


class TelMinimumPhaseSpec(TelPhaseSpec):
    """
    Phase computed from function arguments, choosing the minimum amongst them, while providing an upper limit, meaning
    that the returned phase must not be higher than the upper limit.
    """

    def __init__(
        self,
        maximum_phase: TelPhase = TelPhase.blending_projection,
        argument_extractor: Optional[TelArgumentExtractor] = None,
    ):
        self._maximum_phase = maximum_phase
        self._argument_extractor = argument_extractor

    def to_remote_spec(self) -> Dict[str, Any]:
        result = {"kind": PhaseSpecKind.MINIMUM, "noHigherThan": self._maximum_phase.value}

        if self._argument_extractor:
            result["argumentExtractor"] = self._argument_extractor.to_remote_spec()

        return result

    def get(self, args, context) -> TelPhase:
        if self._argument_extractor:
            extracted_arguments = self._argument_extractor.extract_arguments(args)
            return TelPhase(min([self._maximum_phase] + [arg.phase(context) for arg in extracted_arguments]))
        else:
            return self._maximum_phase
