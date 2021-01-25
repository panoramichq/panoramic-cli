from abc import ABC
from enum import Enum
from typing import Any, Dict, List, Optional

from panoramic.cli.husky.core.taxonomy.enums import ValidationType
from panoramic.cli.husky.core.tel.tel_remote_interface import (
    TelArgumentExtractor,
    TelExpressionSpecGetter,
    TelRemoteSpecProtocol,
)


class TelDataType(Enum):
    UNKNOWN = 'unknown'
    NONE_OPTIONAL = 'none_optional'
    """
    Type returned when optional taxon is not present (out of scope of provided data sources).
    This is compatible with every other type. If some fn requires valid taxon, we will add specific flag/util fn for
    that into validate fn
    """
    ANY = 'any'
    """
    Special data type, which is compatible with any other type.
    """
    STRING = 'string'
    INTEGER = 'integer'
    NUMERIC = 'numeric'
    DATETIME = 'datetime'
    BOOLEAN = 'boolean'


class TelType:
    data_type: TelDataType
    """
    Underlying data type to check compatibility with other types
    """

    is_constant: bool
    """
    Whether this type represents a constant, e.g. string literal
    """

    _VALIDATION_TYPE_MAPPING: Dict[ValidationType, TelDataType] = {
        ValidationType.text: TelDataType.STRING,
        ValidationType.integer: TelDataType.INTEGER,
        ValidationType.numeric: TelDataType.NUMERIC,
        ValidationType.datetime: TelDataType.DATETIME,
        ValidationType.enum: TelDataType.STRING,
        ValidationType.percent: TelDataType.NUMERIC,
        ValidationType.money: TelDataType.NUMERIC,
        ValidationType.url: TelDataType.STRING,
        ValidationType.boolean: TelDataType.BOOLEAN,
        ValidationType.duration: TelDataType.NUMERIC,
        ValidationType.variant: TelDataType.STRING,
    }

    def __init__(self, data_type: TelDataType, is_constant: bool = False):
        self.data_type = data_type
        self.is_constant = is_constant

    def is_any(self) -> bool:
        return self.data_type in {TelDataType.ANY, TelDataType.NONE_OPTIONAL}

    def is_string(self) -> bool:
        return self.data_type in {TelDataType.ANY, TelDataType.STRING, TelDataType.NONE_OPTIONAL}

    def is_number(self) -> bool:
        return self.data_type in {TelDataType.ANY, TelDataType.INTEGER, TelDataType.NUMERIC, TelDataType.NONE_OPTIONAL}

    def is_date(self) -> bool:
        return self.data_type in {TelDataType.ANY, TelDataType.DATETIME, TelDataType.NONE_OPTIONAL}

    def is_integer(self) -> bool:
        return self.data_type in {TelDataType.ANY, TelDataType.INTEGER, TelDataType.NONE_OPTIONAL}

    def is_datetime(self) -> bool:
        return self.data_type in {TelDataType.ANY, TelDataType.DATETIME, TelDataType.NONE_OPTIONAL}

    def is_boolean(self) -> bool:
        return self.data_type in {TelDataType.ANY, TelDataType.BOOLEAN, TelDataType.NONE_OPTIONAL}

    def copy(self, data_type: Optional[TelDataType] = None, is_constant: Optional[bool] = None) -> 'TelType':
        return TelType(
            data_type=data_type if data_type is not None else self.data_type,
            is_constant=is_constant if is_constant is not None else self.is_constant,
        )

    @classmethod
    def from_taxon_validation_type(cls, taxon_validation_type: ValidationType) -> 'TelType':
        # Convert taxon validation type to TEL data type and return STRING for unknown types
        tel_data_type = TelType._VALIDATION_TYPE_MAPPING.get(taxon_validation_type, TelDataType.STRING)
        return TelType(data_type=tel_data_type, is_constant=False)

    @classmethod
    def return_common_type(cls, tel_types: List['TelType']) -> 'TelType':
        if len(tel_types) == 0:
            return TelType(TelDataType.UNKNOWN)

        only_constants = all({t.is_constant for t in tel_types})
        known_tel_types = [
            type
            for type in tel_types
            if not (type.data_type is TelDataType.ANY or type.data_type is TelDataType.NONE_OPTIONAL)
        ]

        if all({t.is_any() for t in tel_types}):
            # All data types are ANY
            return TelType(TelDataType.ANY, is_constant=only_constants)
        elif len({t.data_type for t in known_tel_types}) == 1:
            # All data types are the same
            return TelType(known_tel_types[0].data_type, is_constant=only_constants)
        elif all({t.is_number() for t in known_tel_types}):
            # All data types are numeric-like
            return TelType(TelDataType.NUMERIC, is_constant=only_constants)
        else:
            # If there is no common data type, return UNKNOWN
            return TelType(TelDataType.UNKNOWN, is_constant=only_constants)

    @classmethod
    def are_compatible_data_types(cls, tel_types: List['TelType']) -> bool:
        # Data types are compatible when common return type is other than UNKNOWN
        return cls.return_common_type(tel_types).data_type is not TelDataType.UNKNOWN


class TelReturnTypeSpec(TelRemoteSpecProtocol, TelExpressionSpecGetter[TelType], ABC):
    pass


class TelFixedReturnTypeSpec(TelReturnTypeSpec):
    """
    Fixed, stable return type, nothing to compute from function arguments, or otherwise.
    """

    def __init__(self, data_type: TelDataType, is_constant: bool):
        self._data_type = data_type
        self._is_constant = is_constant

    def get(self, args, context) -> TelType:
        return TelType(self._data_type, self._is_constant)

    def to_remote_spec(self) -> Dict[str, Any]:
        return {"kind": "fixed", "dataType": self._data_type.value, "isConstant": self._is_constant}


class TelCopyReturnTypeSpec(TelReturnTypeSpec):
    """
    Copy return type from some function arguments, while optionally changing either data_type or is_constant of the result.
    Similar behavior than TelType.copy()
    """

    def __init__(
        self,
        data_type: Optional[TelDataType] = None,
        is_constant: Optional[bool] = None,
        argument_extractor: Optional[TelArgumentExtractor] = None,
    ):
        self._data_type = data_type
        self._is_constant = is_constant
        self._argument_extractor = argument_extractor

    def get(self, args, context) -> TelType:
        if self._argument_extractor:
            extracted_arguments = self._argument_extractor.extract_arguments(args)
            return TelType.return_common_type([arg.return_type(context) for arg in extracted_arguments]).copy(
                data_type=self._data_type, is_constant=self._is_constant
            )
        else:
            assert self._data_type  # mypy
            return TelType(data_type=self._data_type, is_constant=self._is_constant or False)

    def to_remote_spec(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {"kind": "copy"}

        if self._data_type:
            result["dataType"] = self._data_type.value

        if self._is_constant is not None:
            result["isConstant"] = self._is_constant

        if self._argument_extractor:
            result["argumentExtractor"] = self._argument_extractor.to_remote_spec()

        return result
