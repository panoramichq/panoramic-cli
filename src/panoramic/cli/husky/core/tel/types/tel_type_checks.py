from abc import ABC, abstractmethod
from typing import List

from panoramic.cli.husky.core.tel.types.tel_types import TelDataType, TelType


class TelTypeCheck(ABC):
    _name: str
    _data_types: List[TelDataType]

    @abstractmethod
    def __call__(self, typ: TelType) -> bool:
        pass

    def __str__(self):
        return self._name

    @property
    def data_types(self) -> List[TelDataType]:
        return self._data_types


class CheckAnyType(TelTypeCheck):
    _name = 'any'
    _data_types = [TelDataType.ANY]

    def __call__(self, typ: TelType) -> bool:
        return True


class CheckStringType(TelTypeCheck):
    _name = 'string'
    _data_types = [TelDataType.STRING]

    def __call__(self, typ: TelType) -> bool:
        return typ.is_string()


class CheckNumberType(TelTypeCheck):
    _name = 'number'
    _data_types = [TelDataType.NUMERIC]

    def __call__(self, typ: TelType) -> bool:
        return typ.is_number()


class CheckIntegerType(TelTypeCheck):
    _name = 'integer'
    _data_types = [TelDataType.INTEGER]

    def __call__(self, typ: TelType) -> bool:
        return typ.is_integer()


class CheckDateType(TelTypeCheck):
    _name = 'date'
    _data_types = [TelDataType.DATETIME]

    def __call__(self, typ: TelType) -> bool:
        return typ.is_date()


class CheckDatetimeType(TelTypeCheck):
    _name = 'datetime'
    _data_types = [TelDataType.DATETIME]

    def __call__(self, typ: TelType) -> bool:
        return typ.is_datetime()


class CheckBooleanType(TelTypeCheck):
    _name = 'boolean'
    _data_types = [TelDataType.BOOLEAN]

    def __call__(self, typ: TelType) -> bool:
        return typ.is_boolean()


class CheckNumberOrStringOrBooleanType(TelTypeCheck):
    _name = 'number or string or boolean'
    _data_types = [TelDataType.NUMERIC, TelDataType.STRING, TelDataType.BOOLEAN]

    def __call__(self, typ: TelType) -> bool:
        return typ.is_number() or typ.is_string() or typ.is_boolean()


class CheckNumberOrStringOrDateType(TelTypeCheck):
    _name = 'number or string or datetime'
    _data_types = [TelDataType.NUMERIC, TelDataType.STRING, TelDataType.DATETIME]

    def __call__(self, typ: TelType) -> bool:
        return typ.is_number() or typ.is_string() or typ.is_datetime()
