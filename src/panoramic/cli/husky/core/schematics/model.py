from enum import Enum
from typing import Any, Dict, Optional, Type

import schematics
import yaml
from schematics.exceptions import ConversionError
from schematics.types import BaseType, UnionType

from panoramic.cli.husky.common.util import FilterEmpty


class CustomValidationError(Exception):
    """
    Custom exception class to be thrown by our custom validations.
    """

    message: Optional[str]

    def __init__(self, message=None):
        self.message = message


class NonEmptyStringType(schematics.types.StringType):
    def validate_not_empty(self, value):
        if not value:
            raise schematics.exceptions.ValidationError("String can not be empty!")


class BaseSchematicsModel(schematics.Model):
    def __getstate__(self):
        # For pickle and depickle
        return self.to_native()

    def __setstate__(self, kwargs: Dict[str, Any]):
        self.__init__(kwargs)  # type: ignore

    def to_yaml(self, filter_none: Optional[bool] = None) -> str:
        """
        Serialize schematics model to yaml string
        """
        serialized = self.to_primitive()

        if filter_none:
            serialized = FilterEmpty.filter_empty(serialized)

        return yaml.dump(serialized, default_flow_style=False)


class SchematicsModel(BaseSchematicsModel):
    def __init__(self, *args, **kwargs):
        # Set defaults, but let them be changed by subclasses.
        kwargs = {'partial': False, 'validate': True, 'strict': False, **kwargs}
        super().__init__(*args, **kwargs)


class EnumType(BaseType):
    """
        Schematics enum type, for serialization and deserialization of enums.
    Example:

    class MyEnum(Enum):
        a = 1
        b = 2

    class ModelWithEnum(SchematicsModel):
        e = EnumType(MyEnum)

    instance = ModelWithEnum(dict(e=1))
    assert instance.e == MyEnum.a

    """

    def __init__(self, enum_class: Type[Enum], **kwargs):
        self.enum_class: Type[Enum] = enum_class
        super().__init__(**kwargs)

    def convert(self, value, context=None):
        if isinstance(value, self.enum_class):
            return value
        try:
            return self.enum_class(value)
        except ValueError as ex:
            # in case we got unsupported value (outside of predefined ENUM), throw a correct error
            raise schematics.exceptions.ValidationError(str(ex)) from ex

    def to_primitive(self, value, context=None):
        return value.value

    def to_native(self, value, context=None):
        return value.value


class UnionNoConversionType(UnionType):
    """
    Use this class if you want to use union, but never ever do a conversion of values.
    Regular union type will convert int 1 to str '1'. See tests for more examples.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def resolve(self, value, context):
        for field in self._types.values():
            try:
                new_value = field.convert(value, context)
                if new_value != value:  #  Make sure the converted value is not different.
                    raise ConversionError('Values after conversion do not match.')
            except ConversionError:
                pass
            else:
                return field, new_value
        return None


class ApiModelSchematics:
    """
    This mixin should be used on API-facing models in order to teach them to transform into an internal model
    """

    def to_internal_model(self) -> schematics.Model:
        """Transform API-facing model into its internal model representation"""
        raise NotImplementedError('Must be implemented')
