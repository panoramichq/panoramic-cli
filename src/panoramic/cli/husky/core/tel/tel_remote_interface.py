from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, Generic, TypeVar

T = TypeVar('T')


class TelRemoteSpecProtocol(ABC):
    """
    Base class defining interface between the diesel service and the auto-completion frontend.
    """

    @abstractmethod
    def to_remote_spec(self) -> Dict[str, Any]:
        pass


class TelExpressionSpecGetter(Generic[T], ABC):
    """
    Getter of any attribute from a TelFunction, from any of the arguments. Could be a phase, return type, invalid value, etc.
    """

    @abstractmethod
    def get(self, args, context) -> T:
        """
        Get the result of the implemented operation
        :param List[TelExpression] args: function arguments
        :param TelRootContext context: Tel context
        :return: the result of the operation
        """
        pass


class ArgumentExtractorSpecKind(Enum):
    SINGLE = "single"
    SLICING = "slicing"


class TelArgumentExtractor(TelRemoteSpecProtocol):
    """
    Extracts desired arguments from the function arguments list, depending on the implementation of the particular subclass details.
    """

    @abstractmethod
    def extract_arguments(self, args):
        """
        Extract argument(s) from the function arguments list
        :param List[TelExpression] args: function arguments
        :return: List[TelExpression]: extracted function argument(s)
        """
        pass


class TelSingleArgumentExtractor(TelArgumentExtractor):
    def __init__(self, position: int):
        self._position = position

    def extract_arguments(self, args):
        try:
            return [args[self._position]]
        except IndexError:
            return []

    def to_remote_spec(self) -> Dict[str, Any]:
        return {"kind": ArgumentExtractorSpecKind.SINGLE, "position": self._position}


class TelSlicingArgumentExtractor(TelArgumentExtractor):
    def __init__(self, start: int):
        self._start = start

    def extract_arguments(self, args):
        try:
            return args[self._start :]
        except IndexError:
            return []

    def to_remote_spec(self) -> Dict[str, Any]:
        return {"kind": ArgumentExtractorSpecKind.SLICING, "start": self._start}


EXTRACT_FIRST_ARGUMENT = TelSingleArgumentExtractor(0)
